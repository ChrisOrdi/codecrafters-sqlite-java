import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RowPredicate {
    String filter;
    Schema schema;
    int colIndex;
    Object expected;
    String op;

    public RowPredicate(String filter, Schema schema) {
        this.filter = filter;
        this.schema = schema;

        // Use regex to split the filter into three parts: column, operator, and value
        Pattern pattern = Pattern.compile("(.+?)\\s*(=|>|<)\\s*'(.+?)'");
        Matcher matcher = pattern.matcher(filter);

        if (!matcher.matches() || matcher.groupCount() != 3) {
            throw new IllegalArgumentException("Invalid filter format: " + filter);
        }

        String colName = matcher.group(1).trim();
        op = matcher.group(2).trim();
        String arg = matcher.group(3).trim();

        colIndex = schema.columnList.stream()
                .filter(c -> c.name().equals(colName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Column not found: " + colName))
                .index();

        if (Character.isDigit(arg.charAt(0))) {
            expected = Integer.parseInt(arg);
        } else {
            expected = arg;
        }
    }

    public boolean eval(Record evalRecord) {
        var recordValue = evalRecord.getValues().get(this.colIndex);
        if ("=".equals(op)) {
            return Objects.equals(recordValue, expected);
        } else {
            throw new UnsupportedOperatorException("Evaluation not implemented for operator " + op);
        }
    }

    public String getFilter() {
        return filter;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getColIndex() {
        return colIndex;
    }

    public Object getExpected() {
        return expected;
    }

    public String getOp() {
        return op;
    }
}

class UnsupportedOperatorException extends RuntimeException {
    public UnsupportedOperatorException(String message) {
        super(message);
    }
}
