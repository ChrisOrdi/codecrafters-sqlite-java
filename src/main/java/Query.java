import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Query {
    String table;
    List<String> columns;
    String whereColumn;
    String whereValue;

    public Query(String table, List<String> columns) {
        this.table = table;
        this.columns = columns;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public void setWhereColumn(String whereColumn) {
        this.whereColumn = whereColumn;
    }

    public String getWhereValue() {
        return whereValue;
    }

    public void setWhereValue(String whereValue) {
        this.whereValue = whereValue;
    }

    public static Query parse(String queryText) {
        Scanner scanner = new Scanner(queryText);
        var select = scanner.next();
        assert select.equalsIgnoreCase("select");
        List<String> columns = new ArrayList<>();
        while (!(scanner.hasNext("from") || scanner.hasNext("FROM"))) {
            var c = scanner.next();
            var index = c.indexOf(',');
            var columnName = index >= 0 ? c.substring(0, index) : c;
            columns.add(columnName);
        }
        var next = scanner.next();
        assert next.equalsIgnoreCase("from");
        var tableName = scanner.next();

        String whereColumn = null;
        String whereValue = null;
        if (scanner.hasNext("where") || scanner.hasNext("WHERE")) {
            scanner.next(); // skip 'where'
            whereColumn = scanner.next();
            scanner.next(); // skip '='
            whereValue = scanner.next();
            if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
                whereValue = whereValue.substring(1, whereValue.length() - 1);
            }
        }

        Query query = new Query(tableName, columns);
        query.setWhereColumn(whereColumn);
        query.setWhereValue(whereValue);
        return query;
    }
}
