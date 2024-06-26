import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
public class DB {
    //    ByteBuffer fileContents;
    String databaseFilePath;
    public DB(String databaseFilePath) throws IOException {
        this.databaseFilePath = databaseFilePath;
        load();
    }
    RandomAccessFile randomAccessFile;
    int pageSize;
    DBInfo dbInfo;
    private void load() throws IOException {
        randomAccessFile = new RandomAccessFile(databaseFilePath, "r");
        this.dbInfo = dbInfo();
    }
    public record DBInfo(int pageSize, int numberOfTables) {}
    public DBInfo dbInfo() throws IOException {
        // The page size is stored at the 16th byte offset, using 2 bytes in
        // big-endian order.
        // '& 0xFFFF' is used to convert the signed short to an unsigned int.
        byte[] headerBytes = new byte[100];
        randomAccessFile.seek(0);
        int bytesRead = randomAccessFile.read(headerBytes);
        assert bytesRead == 100;
        ByteBuffer pageHeader =
                ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
        pageSize = pageHeader.position(16).getShort() & 0xFFFF;
        byte[] pageBuffer = new byte[pageSize];
        randomAccessFile.seek(0);
        bytesRead = randomAccessFile.read(pageBuffer);
        assert bytesRead == pageSize;
        ByteBuffer firstPage =
                ByteBuffer.wrap(pageBuffer).order(ByteOrder.BIG_ENDIAN);
        firstPage.position(100);
        var BtreeHeader = BtreePageHeader.getHeader(firstPage);
        assert BtreeHeader.pageType == 0x0d;
        return new DBInfo(pageSize, BtreeHeader.cellCounts);
    }
    public void printTableNames() throws IOException {
        BtreePage page = getFirstPage();
        List<String> tableNames = new ArrayList<>();
        ByteBuffer pageContents =
                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
            ByteBuffer cellPayload =
                    ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            tableNames.add(String.valueOf(record.getValues().get(2)));
        }
        System.out.println(String.join(" ", tableNames));
    }
    public int countRows(String table) throws IOException {
        var tablePage = getTablePage(table);
        return countRows(tablePage);
    }
    public int countRows(BtreePage page) throws IOException {
        if (page.btreePageHeader.pageType == 0x0d) {
            return page.btreePageHeader.cellCounts;
        } else {
            int count = 0;
            ByteBuffer pageContents =
                    ByteBuffer.wrap(page.getPageContents()).order(ByteOrder.BIG_ENDIAN);
            for (var cellOffset : page.cellPointerArray) {
                pageContents.position(cellOffset);
                var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
                var pageNumber = cell.leftChildPointer;
                var childPage = getNthPage(pageNumber);
                count += countRows(childPage);
            }
            var rightmostPage = getNthPage(page.btreePageHeader.rightMostPointer);
            count += countRows(rightmostPage);
            return count;
        }
    }
    private BtreePage getTablePage(String table) throws IOException {
        BtreePage page = getFirstPage();
        byte rootPageIndex = 0;
        ByteBuffer pageContents =
                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
            ByteBuffer cellPayload =
                    ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            if (record.getValues().get(2).equals(table)) {
                rootPageIndex = (byte)record.getValues().get(3);
                break;
            }
        }
        return getNthPage(rootPageIndex);
    }
    public List<String[]> runQuery(Query query) throws IOException {
        load();
        var firstPage = BtreePage.readPage(randomAccessFile, pageSize, 1);
        var schema = Schema.loadSchema(firstPage, query.getTable());
        var columnIndexes = getColumnIndexes(schema, query);
        var tablePage = getTablePage(query.getTable());
        RowPredicate rowPredicate = null;
        List<String[]> resultSet = new ArrayList<>();
        if (!query.filter.isBlank()) {
            rowPredicate = new RowPredicate(query.filter, schema);
            if (schema.index != null &&
                    schema.index.colIndex() == rowPredicate.colIndex) {
                var indexRootPageNumber = schema.index.pageNumber();
                var indexPage =
                        BtreePage.readPage(randomAccessFile, pageSize, indexRootPageNumber);
                List<Record> indexResultSet2 = new ArrayList<>();
                queryIndexOpt(indexPage, rowPredicate, indexResultSet2);
                executeIndexedSearch(tablePage, indexResultSet2, columnIndexes,
                        resultSet);
                return resultSet;
            }
        }
        executeQuery(tablePage, columnIndexes, rowPredicate, resultSet);
        return resultSet;
    }
    record IndexedRecord(long rowId, Record re) {}
    private IndexedRecord indexedSearch(BtreePage page, Record key)
            throws IOException {
        page.popCells();
        byte pageType = page.btreePageHeader.pageType;
        Object k = key.getValues().get(1);
        long searchedValue = switch (k) {
            case Integer i -> Long.valueOf(i);
            case Long l -> l;
            case Short s -> s;
            default -> throw new IllegalStateException("Unexpected value: " + k);
        };
        Long[] keysInCell = Arrays.stream(page.cellArray).map(c -> c.getRowId().value()).toArray(Long[]::new);
        //interior
        if (pageType == 0x05) {
            //search for first key index in array that is greater or equal to searched key
            for (int i = 0; i < keysInCell.length; ++i) {
                if(searchedValue <= keysInCell[i]){
                    var childPage = getNthPage(page.cellArray[i].leftChildPointer);
                    return indexedSearch(childPage, key);
                }
            }
            if(searchedValue >= keysInCell[keysInCell.length-1]){
                var rightMostPage = getNthPage(page.btreePageHeader.rightMostPointer);
                return indexedSearch(rightMostPage, key);
            }
        } else { //leaf
            for (int i = 0; i < keysInCell.length; ++i) {
                if (searchedValue == keysInCell[i]) {
                    var rowId = page.cellArray[i].rowId.value();
                    return new IndexedRecord(rowId, page.records[i]);
                }
            }
        }
        return null;
    }
    private void executeIndexedSearch(BtreePage page, List<Record> indexedKeys, List<Schema.Column> selectedColumns, List<String[]> resultSet) throws IOException {
        for(var key : indexedKeys){
            var searchResult = indexedSearch(page, key);
            if (searchResult != null) {
                String[] result = new String[selectedColumns.size()];
                for (int i = 0; i < selectedColumns.size(); ++i) {
                    var colIndex = selectedColumns.get(i).index();
                    var colValue =
                            selectedColumns.get(i).isPK()
                                    ? String.valueOf(searchResult.rowId)
                                    : String.valueOf(
                                    searchResult.re.getValue(colIndex));
                    result[i] = colValue;
                }
                resultSet.add(result);
            }
        }
    }
    private void printIndexedResults(List<Record> indexResultSet) {
        for (Record record : indexResultSet) {
            for (int i = 0; i < record.values.size(); ++i) {
                System.out.printf("type: %d - value: %s | ",
                        record.serialTypes.get(i),
                        record.getValues().get(i));
            }
            System.out.println();
        }
    }
    private Cell getNthCell(ByteBuffer pageContents, short cellPointer,
                            byte pageType) {
        pageContents.position(cellPointer);
        return Cell.readCell(pageContents, pageType);
    }
    private int recordCompare(String searchedValue, Record record) {
        String recordValue = (String)record.getValues().get(0);
        return searchedValue.compareTo(recordValue);
    }
    private Record getRecord(Cell cell) {
        ByteBuffer buffer =
                ByteBuffer.wrap(cell.payload).order(ByteOrder.BIG_ENDIAN);
        return Record.readRecord(buffer);
    }
    private void queryIndexOpt(
            BtreePage page, RowPredicate rowPredicate,
            List<Record> resultSet) throws IOException {
        page.popCells();
        byte pageType = page.btreePageHeader.pageType;
        String searchedValue = (String)rowPredicate.getExpected();
        String[] keysInCell = Arrays.stream(page.records)
                .map(r -> (String)r.getValue(0))
                .toArray(String[] ::new);
        // interior
        if (pageType == 0x02) {
            // search for first key index in array that is greater or equal
            // to searched key
            for (int i = 0; i < keysInCell.length; ++i) {
                var cellKey = keysInCell[i];
                if (Objects.nonNull(cellKey) &&
                        searchedValue.compareTo(cellKey) <= 0) {
                    if (searchedValue.equals(cellKey)) {
                        resultSet.add(page.records[i]);
                    }
                    // continue search in child page
                    var childPage =
                            getNthPage(page.cellArray[i].leftChildPointer);
                    queryIndexOpt(childPage, rowPredicate, resultSet);
                }
            }
            if (searchedValue.compareTo(keysInCell[keysInCell.length - 1]) >
                    0) {
                var rightMostPage =
                        getNthPage(page.btreePageHeader.rightMostPointer);
                queryIndexOpt(rightMostPage, rowPredicate, resultSet);
            }
        } else { // leaf
            for (int i = 0; i < keysInCell.length; ++i) {
                if (searchedValue.compareTo(keysInCell[i]) <= 0) {
                    if (searchedValue.equals(keysInCell[i])) {
                        resultSet.add(page.records[i]);
                    }
                }
            }
        }
    }
    private void queryIndex(
            BtreePage page, List<Schema.Column> columnIndices,
            RowPredicate rowPredicate, List<Record> indexResultSet)
            throws IOException {
        page.popCells();
        String searchedValue = (String)rowPredicate.getExpected();
        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents)
                .order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell =
                    Cell.readCell(pageContents, page.btreePageHeader.pageType);
            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload())
                    .order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            String recordValue = (String)record.getValues().get(0);
            var compareResult = searchedValue.compareTo(recordValue);
            if (compareResult == 0) {
                indexResultSet.add(record);
            }
            if (compareResult <= 0 && cell.type == 0x02) {
                var childPageNumber = cell.leftChildPointer;
                BtreePage childPage = getNthPage(childPageNumber);
                queryIndex(childPage, columnIndices, rowPredicate,
                        indexResultSet);
            }
        }
        if (page.btreePageHeader.pageType == 0x02) {
            BtreePage rightMostChild =
                    getNthPage(page.btreePageHeader.rightMostPointer);
            queryIndex(rightMostChild, columnIndices, rowPredicate,
                    indexResultSet);
        }
    }
    private void executeQuery(
            BtreePage page, List<Schema.Column> columnIndices,
            RowPredicate rowPredicate, List<String[]> resultSet)
            throws IOException {
        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents)
                .order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell =
                    Cell.readCell(pageContents, page.btreePageHeader.pageType);
            // leaf table
            if (cell.type == 0x0d) {
                ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload())
                        .order(ByteOrder.BIG_ENDIAN);
                var record = Record.readRecord(cellPayload);
                var includeRowInResultSet =
                        rowPredicate == null || rowPredicate.eval(record);
                if (includeRowInResultSet) {
                    String[] row = new String[columnIndices.size()];
                    for (int i = 0; i < columnIndices.size(); ++i) {
                        var colIndex = columnIndices.get(i).index();
                        var colValue = columnIndices.get(i).isPK()
                                ? cell.rowId.value()
                                : record.getValues().get(colIndex);
                        row[i] = String.valueOf(colValue);
                    }
                    resultSet.add(row);
                }
            } else if (cell.type == 0x05) {
                var childPageNumber = cell.leftChildPointer;
                BtreePage childPage = getNthPage(childPageNumber);
                executeQuery(childPage, columnIndices, rowPredicate,
                        resultSet);
            } else {
                throw new RuntimeException(
                        "not implemented for page of type: " + cell.type);
            }
        }
        if (page.btreePageHeader.pageType == 0x05) {
            BtreePage rightMostChild =
                    getNthPage(page.btreePageHeader.rightMostPointer);
            executeQuery(rightMostChild, columnIndices, rowPredicate,
                    resultSet);
        }
    }
    private List<Schema.Column> getColumnIndexes(Schema schema,
                                                 Query query) {
        List<Schema.Column> indexes = new ArrayList<>();
        for (String selectedColumn : query.getColumns()) {
            var col = schema.columnList.stream()
                    .filter(c -> c.name().equals(selectedColumn))
                    .findAny();
            if (col.isEmpty()) {
                throw new RuntimeException("column not found " +
                        selectedColumn);
            }
            indexes.add(col.get());
        }
        return indexes;
    }
    private BtreePage getFirstPage() throws IOException {
        return BtreePage.readPage(randomAccessFile, pageSize, 1);
    }
    private BtreePage getNthPage(int n) throws IOException {
        return BtreePage.readPage(randomAccessFile, pageSize, n);
    }
}