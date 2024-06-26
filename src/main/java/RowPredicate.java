import java.util.Objects;
public class RowPredicate {
    String filter;
    Schema schema;
    int colIndex;
    Object expected;
    String op;
    public RowPredicate(String filter, Schema schema) {
        this.filter = filter;
        this.schema = schema;
        var filterParts = filter.trim().splitWithDelimiters("[=><]", 0);
        var colName = filterParts[0].trim();
        op = filterParts[1].trim();
        var arg = filterParts[2].trim();
        colIndex = schema.columnList.stream()
                .filter(c -> c.name().equals(colName))
                .findAny()
                .get()
                .index();
        if (Character.isDigit(arg.charAt(0))) {
            expected = Integer.parseInt(arg);
        } else {
            expected = arg.substring(1, arg.lastIndexOf("'"));
        }
    }
    public boolean eval(Record record) {
        var recordValue = record.getValues().get(this.colIndex);
        switch (op) {
            case "=":
            return Objects.equals(recordValue, expected);
            default:
                throw new RuntimeException("evaluation not implemented for operator " +
                        op);
        }
    }
public String getFilter() { return filter; }
public Schema getSchema() { return schema; }
public int getColIndex() { return colIndex; }
public Object getExpected() { return expected; }
public String getOp() { return op; }
}
