import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataBase {
    ByteBuffer fileContents;
    String databaseFilePath;

    public DataBase(String databaseFilePath) {
        this.databaseFilePath = databaseFilePath;
    }

    public void load() throws IOException {
        fileContents = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath))).order(ByteOrder.BIG_ENDIAN);
    }

    public record DBInfo(int pageSize, int numberOfTables) {}

    public DBInfo dbInfo() {
        int pageSize = fileContents.position(16).getShort() & 0xFFFF;
        ByteBuffer firstPage = fileContents.position(100);
        var pageHeader = BtreePageHeader.getHeader(firstPage);
        assert pageHeader.pageType == 0x0d;
        return new DBInfo(pageSize, pageHeader.cellCounts);
    }

    public void printTableNames() throws IOException {
        BtreePage page = getFirstPage();
        List<String> tableNames = new ArrayList<>();
        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            tableNames.add(String.valueOf(record.getValues().get(2)));
        }
        System.out.println(String.join(", ", tableNames));
    }

    public int countRows(String table) throws IOException {
        var tablePage = getTablePage(table);
        return tablePage.getBtreePageHeader().cellCounts;
    }

    private BtreePage getTablePage(String table) throws IOException {
        BtreePage page = getFirstPage();
        byte rootPageIndex = 0;
        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            if (record.getValues().get(2).equals(table)) {
                rootPageIndex = (byte) record.getValues().get(3);
                break;
            }
        }
        return getNthPage(rootPageIndex);
    }

    public List<String[]> runQuery(Query query) throws IOException {
        var firstPage = BtreePage.readPage(fileContents, 1);
        var schema = Schema.loadSchema(firstPage, query.getTable());
        var columnIndexes = getColumnIndexes(schema, query);
        var tablePage = getTablePage(query.getTable());
        ByteBuffer pageContents = ByteBuffer.wrap(tablePage.pageContents).order(ByteOrder.BIG_ENDIAN);
        List<String[]> result = new ArrayList<>();
        for (var cellOffset : tablePage.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, tablePage.btreePageHeader.pageType);
            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            String[] row = new String[columnIndexes.size()];
            for (int i = 0; i < columnIndexes.size(); ++i) {
                row[i] = String.valueOf(record.getValues().get(columnIndexes.get(i)));
            }
            result.add(row);
        }
        return result;
    }

    private List<Integer> getColumnIndexes(Schema schema, Query query) {
        List<Integer> indexes = new ArrayList<>();
        for (String selectedColumn : query.getColumns()) {
            var col = schema.columnList.stream().filter(c -> c.name().equals(selectedColumn)).findAny();
            if (col.isEmpty()) {
                throw new RuntimeException("column not found " + selectedColumn);
            }
            indexes.add(col.get().index());
        }
        return indexes;
    }

    private BtreePage getFirstPage() throws IOException {
        return BtreePage.readPage(fileContents, 1);
    }

    private BtreePage getNthPage(int n) {
        return BtreePage.readPage(fileContents, n);
    }

    // Helper classes and methods for BtreePage, Cell, Record, Schema, Query, etc.
    // should be defined elsewhere in your project.
}
