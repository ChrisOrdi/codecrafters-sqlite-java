import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
            var schemaRecord = Record.readRecord(cellPayload);

            schema = processSchemaRecord(schemaRecord, schema, table);
        }

        if (schema == null) {
            throw new SchemaLoadingException("Error loading schema for table: " + table);
        }
        return schema;
    }

    private static Schema processSchemaRecord(Record schemaRecord, Schema schema, String table) {
        var objectType = (String) schemaRecord.getValues().get(0);
        var objectName = (String) schemaRecord.getValues().get(2);
        var objectDef = (String) schemaRecord.getValues().get(4);
        int pageNumber = extractPageNumber(schemaRecord);

        if (objectName.equals(table)) {
            switch (objectType) {
                case "table" -> {
                    List<Column> columns = parseColumns(objectDef);
                    schema = new Schema(objectName, columns, pageNumber);
                }
                case "index" -> {
                    if (schema instanceof Schema s) {
                        s.index = parseIndex(s, pageNumber, objectName, objectDef);
                    }
                }
                default -> throw new SchemaLoadingException("Unknown object type: " + objectType);
            }
        }
        return schema;
    }

    private static int extractPageNumber(Record schemaRecord) {
        Object val3 = schemaRecord.getValues().get(3);
        return switch (val3) {
            case Integer integer -> integer;
            case Byte byteValue -> byteValue.intValue();
            default -> throw new UnexpectedValueTypeException("Unexpected type for val3");
        };
    }

    protected static Index parseIndex(Schema schema, int indexPageNumber, String indexName, String indexDef) {
        int openParenIdx = indexDef.indexOf('(');
        int closeParenIdx = indexDef.indexOf(')');
        String colName = indexDef.substring(openParenIdx + 1, closeParenIdx);
        var colOptional = schema.columnList.stream()
                .filter(c -> c.name.equals(colName))
                .findAny();
        if (colOptional.isEmpty()) {
            throw new SchemaLoadingException("Column not found: " + colName);
        }
        var colIndex = colOptional.get().index;
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

class UnexpectedValueTypeException extends RuntimeException {
    public UnexpectedValueTypeException(String message) {
        super(message);
    }
}

class SchemaLoadingException extends RuntimeException {
    public SchemaLoadingException(String message) {
        super(message);
    }
}
