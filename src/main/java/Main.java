import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
public class Main {
    public static void main (String[]args) throws IOException {
      if (args.length < 2) {
        System.out.println("Missing <database path> and <command>");
        return;
      }
      String databaseFilePath = args[0];
      String command = args[1];
      DB db = new DB(databaseFilePath);
      switch (command) {
        case ".dbinfo" -> {
          try {
            var dbInfo = db.dbInfo();
            System.out.printf("database page size: %d\n", dbInfo.pageSize());
            System.out.printf("number of tables: %d\n", dbInfo.numberOfTables());
          } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
          }
        }
        case ".tables" -> {
          try {
            db.printTableNames();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        default -> {
          var query = Query.parse(command);
          if (query.getColumns().size() == 1 && query.getColumns().get(0).equalsIgnoreCase("count(*)")) {
            try {
              var c = db.countRows(query.getTable());
              System.out.println(c);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          } else {
            try {
              var result = db.runQuery(query);
              for (var res : result) {
                System.out.println(String.join("|", res));
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
  }