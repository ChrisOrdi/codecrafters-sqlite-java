import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
public class Query {
    String table;
    List<String> columns;
    String filter;
    public Query(String table, List<String> columns) {
        this.table = table;
        this.columns = columns;
    }
    public Query(String table, List<String> columns, String filter) {
        this.table = table;
        this.columns = columns;
        this.filter = filter;
    }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public static Query parse(String sql) {
        String[] parts = sql.split(" ");
        List<String> partsList = Arrays.asList(parts);
        int selectIndex = getIndexCaseInsensite("SELECT", partsList);
        int fromIndex = getIndexCaseInsensite("FROM", partsList);
        int whereIndex = getIndexCaseInsensite("WHERE", partsList);
        String table = partsList.get(fromIndex + 1);
        List<String> columns = new ArrayList<>();
        for (int i = selectIndex + 1; i < fromIndex; ++i) {
            var col = partsList.get(i);
            var index = col.indexOf(',');
            var columnName = index >= 0 ? col.substring(0, index) : col;
            columns.add(columnName);
        }
        if (whereIndex != -1) {
            String filter =
                    String.join(" ", partsList.subList(whereIndex + 1, partsList.size()));
            return new Query(table, columns, filter);
        } else {
            return new Query(table, columns, "");
        }
    }
    private static int getIndexCaseInsensite(String str, List<String> partsList) {
        int index = partsList.indexOf(str);
        if (index == -1) {
            index = partsList.indexOf(str.toLowerCase());
        }
        return index;
    }
    @Override
    public String toString() {
        return "Query{"
                + "table='" + table + '\'' + ", columns=" + columns + ", filter='" +
                filter + '\'' + '}';
    }
}