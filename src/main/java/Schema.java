import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
public class Schema {
    String tableName;
    public record Column(String name, String type, Integer index) {}
    List<Column> columnList;
    public Schema(String tableName, List<Column> columnList) {
        this.tableName = tableName;
        this.columnList = columnList;
    }
    public static Schema loadSchema(BtreePage page, String table)
            throws IOException {
        byte pageType = page.btreePageHeader.pageType;
        ByteBuffer pageContents =
                ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        String createTable = null;
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, pageType);
            ByteBuffer cellPayload =
                    ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            if (record.getValues().get(2).equals(table)) {
                createTable = (String)record.getValues().get(4);
                break;
            }
        }
        if (createTable == null) {
            throw new RuntimeException("error loading schema for table: " + table);
        }
        List<Column> columns = parseColumns(createTable);
        return new Schema(table, columns);
    }
    protected static List<Column> parseColumns(String tableDefinition) {
        var openParenIndex = tableDefinition.indexOf('(');
        var closeParenIndex = tableDefinition.indexOf(')');
        var columnDefList =
                tableDefinition.substring(openParenIndex + 1, closeParenIndex);
        var columns = columnDefList.split(",");
        var result = new ArrayList<Column>();
        for (int i = 0; i < columns.length; ++i) {
            var colDef = columns[i].trim().split(" ", 0);
            result.add(new Column(colDef[0].trim(), colDef[1].trim(), i));
        }
        return result;
    }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public List<Column> getColumnList() { return columnList; }
    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }
    @Override
    public String toString() {
        return "Schema{"
                + "tableName='" + tableName + '\'' + ", columnList=" + columnList + '}';
    }
}
