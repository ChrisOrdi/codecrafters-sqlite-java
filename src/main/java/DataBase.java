//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Objects;
//public class DataBase {
//    ByteBuffer fileContents;
//    String databaseFilePath;
//    public DataBase(String databaseFilePath) {
//        this.databaseFilePath = databaseFilePath;
//    }
//    public void load() throws IOException {
//        fileContents =
//                ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
//                        .order(ByteOrder.BIG_ENDIAN);
//    }
//    public record DBInfo(int pageSize, int numberOfTables) {}
//    public DBInfo dbInfo() {
//        // The page size is stored at the 16th byte offset, using 2 bytes in
//        // big-endian order.
//        // '& 0xFFFF' is used to convert the signed short to an unsigned int.
//        int pageSize = fileContents.position(16).getShort() & 0xFFFF;
//        ByteBuffer firstPage = fileContents.position(100);
//        var pageHeader = BtreePageHeader.getHeader(firstPage);
//        // leaf b-tree page
//        assert pageHeader.pageType == 0x0d;
//        return new DBInfo(pageSize, pageHeader.cellCounts);
//    }
//    public void printTableNames() throws IOException {
//        BtreePage page = getFirstPage();
//        List<String> tableNames = new ArrayList<>();
//        ByteBuffer pageContents =
//                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
//        for (var cellOffset : page.cellPointerArray) {
//            pageContents.position(cellOffset);
//            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
//            ByteBuffer cellPayload =
//                    ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
//            var record = Record.readRecord(cellPayload);
//            tableNames.add(String.valueOf(record.getValues().get(2)));
//        }
//        System.out.println(String.join(" ", tableNames));
//    }
//    public int countRows(String table) throws IOException {
//        var tablePage = getTablePage(table);
//        return countRows(tablePage);
//    }
//    public int countRows(BtreePage page) {
//        if (page.btreePageHeader.pageType == 0x0d) {
//            return page.btreePageHeader.cellCounts;
//        } else {
//            int count = 0;
//            ByteBuffer pageContents =
//                    ByteBuffer.wrap(page.getPageContents()).order(ByteOrder.BIG_ENDIAN);
//            for (var cellOffset : page.cellPointerArray) {
//                pageContents.position(cellOffset);
//                var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
//                var pageNumber = cell.leftChildPointer;
//                var childPage = getNthPage(pageNumber);
//                count += countRows(childPage);
//            }
//            var rightmostPage = getNthPage(page.btreePageHeader.rightMostPointer);
//            count += countRows(rightmostPage);
//            return count;
//        }
//    }
//    private BtreePage getTablePage(String table) throws IOException {
//        BtreePage page = getFirstPage();
//        byte rootPageIndex = 0;
//        ByteBuffer pageContents =
//                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
//        for (var cellOffset : page.cellPointerArray) {
//            pageContents.position(cellOffset);
//            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
//            ByteBuffer cellPayload =
//                    ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
//            var record = Record.readRecord(cellPayload);
//            if (record.getValues().get(2).equals(table)) {
//                rootPageIndex = (byte)record.getValues().get(3);
//                break;
//            }
//        }
//        return getNthPage(rootPageIndex);
//    }
//    public List<String[]> runQuery(Query query) throws IOException {
//        var firstPage = BtreePage.readPage(fileContents, 1);
//        var schema = Schema.loadSchema(firstPage, query.getTable());
//        var columnIndexes = getColumnIndexes(schema, query);
//        var tablePage = getTablePage(query.getTable());
//        RowPredicate rowPredicate = null;
//        List<String[]> resultSet = new ArrayList<>();
//        if (!query.filter.isBlank()) {
//            rowPredicate = new RowPredicate(query.filter, schema);
//        }
//        executeQuery(tablePage, columnIndexes, rowPredicate, resultSet);
//        return resultSet;
//    }
//    private void executeQuery(BtreePage page, List<Schema.Column> columnIndices,
//                              RowPredicate rowPredicate,
//                              List<String[]> resultSet) {
//        ByteBuffer pageContents =
//                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
//
//        for (var cellOffset : page.cellPointerArray) {
//            pageContents.position(cellOffset);
//
//            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
//
//            // leaf table
//            if (cell.type == 0x0d) {
//                ByteBuffer cellPayload =
//                        ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
//                var record = Record.readRecord(cellPayload);
//                var includeRowInResultSet =
//                        rowPredicate == null || rowPredicate.eval(record);
//                if (includeRowInResultSet) {
//                    String[] row = new String[columnIndices.size()];
//                    for (int i = 0; i < columnIndices.size(); ++i) {
//                        var colIndex = columnIndices.get(i).index();
//                        var colValue = columnIndices.get(i).isPK()
//                                ? cell.rowId.value()
//                                : record.getValues().get(colIndex);
//                        row[i] = String.valueOf(colValue)
//                    }
//                    resultSet.add(row);
//                }
//            } else if (cell.type == 0x05) {
//                var childPageNumber = cell.leftChildPointer;
//                BtreePage childPage = getNthPage(childPageNumber);
//                executeQuery(childPage, columnIndices, rowPredicate, resultSet);
//            } else {
//                throw new RuntimeException("not implemented for page of type: " +
//                        cell.type);
//            }
//        }
//        if (page.btreePageHeader.pageType == 0x05) {
//            BtreePage rightMostChild =
//                    getNthPage(page.btreePageHeader.rightMostPointer);
//            executeQuery(rightMostChild, columnIndices, rowPredicate, resultSet);
//        }
//    }
//    private List<Schema.Column> getColumnIndexes(Schema schema, Query query) {
//        List<Schema.Column> indexes = new ArrayList<>();
//        for (String selectedColumn : query.getColumns()) {
//            var col = schema.columnList.stream()
//                    .filter(c -> c.name().equals(selectedColumn))
//                    .findAny();
//            if (col.isEmpty()) {
//                throw new RuntimeException("column not found " + selectedColumn);
//            }
//            indexes.add(col.get());
//        }
//        return indexes;
//    }
//    private BtreePage getFirstPage() throws IOException {
//        return BtreePage.readPage(fileContents, 1);
//    }
//    private BtreePage getNthPage(int n) {
//        return BtreePage.readPage(fileContents, n);
//    }
//}