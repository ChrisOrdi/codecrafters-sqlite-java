import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Schema {
    String tableName;
    Integer pageNumber;
    List<Column> columnList;
    Index index;

    public record Column(String name, String type, Integer index, boolean isPK) {}
    public record Index(String name, String column, int colIndex, int pageNumber) {}

    public Schema(String tableName, List<Column> columnList, int pageNumber) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.pageNumber = pageNumber;
    }

    public Schema(String tableName, List<Column> columnsList, int pageNumber, Index index) {
        this.tableName = tableName;
        this.columnList = columnsList;
        this.pageNumber = pageNumber;
        this.index = index;
    }

    public static Schema loadSchema(BtreePage page, String table) throws IOException {
        byte pageType = page.btreePageHeader.pageType;
        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
        Schema schema = null;
        for (var cellOffset : page.cellPointerArray) {
            pageContents.position(cellOffset);
            var cell = Cell.readCell(pageContents, pageType);
            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
            var record = Record.readRecord(cellPayload);
            var objectType = (String) record.getValues().get(0);
            var objectName = (String) record.getValues().get(2);
            var objectDef = (String) record.getValues().get(4);
            Object val3 = record.getValues().get(3);
            int pageNumber;
            if (val3 instanceof Integer) {
                pageNumber = (Integer) val3;
            } else if (val3 instanceof Byte) {
                pageNumber = (Byte) val3;
            } else {
                throw new RuntimeException("unexpected type for val3");
            }
            var objectPageNumber = pageNumber;

            if (objectName.equals(table)) {
                switch (objectType) {
                    case "table" -> {
                        List<Column> columns = parseColumns(objectDef);
                        schema = new Schema(objectName, columns, objectPageNumber);
                    }
                    case "index" -> {
                        assert schema != null;
                        schema.index = parseIndex(schema, objectPageNumber, objectName, objectDef);
                    }
                }
            }
        }

        if (schema == null) {
            throw new RuntimeException("error loading schema for table: " + table);
        }
        return schema;
    }

    protected static Index parseIndex(Schema schema, int indexPageNumber, String indexName, String indexDef) {
        int openParenIdx = indexDef.indexOf('(');
        int closeParenIdx = indexDef.indexOf(')');
        String colName = indexDef.substring(openParenIdx + 1, closeParenIdx);
        var colIndex = schema.columnList.stream()
                .filter(c -> c.name.equals(colName))
                .findAny()
                .get()
                .index;
        return new Index(indexName, colName, colIndex, indexPageNumber);
    }

    protected static List<Column> parseColumns(String tableDefinition) {
        var openParenIndex = tableDefinition.indexOf('(');
        var closeParenIndex = tableDefinition.indexOf(')');
        var columnDefList = tableDefinition.substring(openParenIndex + 1, closeParenIndex);
        var columns = columnDefList.split(",");
        var result = new ArrayList<Column>();
        for (int i = 0; i < columns.length; ++i) {
            var colDef = columns[i].trim().split(" ", 0);
            result.add(new Column(colDef[0].trim(), colDef[1].trim(), i, columns[i].toUpperCase().contains("INTEGER PRIMARY KEY")));
        }
        return result;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public Integer getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "tableName='" + tableName + '\'' +
                ", pageNumber=" + pageNumber +
                ", columnList=" + columnList +
                ", index=" + index +
                '}';
    }
}
