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
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }
    String databaseFilePath = args[0];
    String command = args[1];
    DataBase database = new DataBase(databaseFilePath);
    switch (command) {
      case ".dbinfo" ->
      {
        try
        {
          // You can use print statements as follows for debugging, they'll be visible when running tests.
          //System.out.println("Logs from your program will appear here!");

          byte[] header = Files.readAllBytes(Path.of(databaseFilePath));

          // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order. '& 0xFFFF' is used to convert the signed short to an unsigned int.
          int pageSize = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN).position(16).getShort() & 0xFFFF;

          // Uncomment this block to pass the first stage
          System.out.println("database page size: " + pageSize + "\n");

          byte[] allBytes = Files.readAllBytes(Path.of(databaseFilePath));
          ByteBuffer allBytesBuffer = ByteBuffer.wrap(allBytes);

          pageSize = allBytesBuffer.slice(0, 100).position(16).getShort() & 0xFFFF;
          ByteBuffer sqliteSchema = allBytesBuffer.slice(100, pageSize);

          byte bTreePageType = sqliteSchema.get();
          short startOfFirstFreeBlock = sqliteSchema.getShort();
          short numOfCells = sqliteSchema.getShort();
          System.out.println("number of tables: " + numOfCells + "\n");
        }
        catch (IOException e)
        {
          System.out.println("Error reading file: " + e.getMessage());
        }
      }

      case ".tables" ->
      {
        try
        {
          printTables(databaseFilePath);
        }
        catch (IOException e)
        {
          System.out.println("Error reading file: " + e.getMessage());
        }
      }

//      default -> System.out.println("Missing or invalid command passed: " + command);
      default ->
      {
        var queryTokens = command.split(" ");

        if (queryTokens.length == 4)
        {
          var tableName = queryTokens[3];
          try
          {
            database.load();
            var c = database.countRows(tableName);
            System.out.println(c);
          }
          catch (IOException e)
          {
            throw new RuntimeException(e);
          }
        }
        else
        {
          throw new RuntimeException("invalid command");
        }
      }
    }
  }

  private static void printTables(String databaseFilePath) throws IOException
  {
    ByteBuffer fileContents = getContents(databaseFilePath).position(100);
    ByteBuffer firstPage = fileContents.position(100);
    var pageHeader = BtreePageHeader.getHeader(firstPage);
    firstPage.position(pageHeader.getGetStartOfCellContentArea());
    List<List<String>> records = new Stack<>();
    for(int i=0;i<pageHeader.cellCounts; ++i)
    {
      records.add(printCell(firstPage));
    }
    Collections.reverse(records);
    System.out.println(String.join(" ", records.stream().map(rec -> rec.get(2)).toList()));
  }
  private static ByteBuffer getContents(String databaseFilePath) throws IOException
  {
    return ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath))).order(ByteOrder.BIG_ENDIAN);
  }
  private static List<String> printCell(ByteBuffer cellArray)
  {
    //varint
    VarInt bytesOfPayload = Cell.from(cellArray);
    //varint
    VarInt rowId = Cell.from(cellArray);
    //payload
    byte[] payload = new byte[(int)bytesOfPayload.value()];
    cellArray.get(payload);
    ByteBuffer recordBuf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    return printRecord(recordBuf);
    //if everything fits in the page, omit this.
    //int firstPageOverflowList = cellArray.getInt();
  }
  private static List<String> printRecord(ByteBuffer buffer)
  {
    //header
    var sizeOfHeader = Cell.from(buffer);
    long remaining = sizeOfHeader.value() - sizeOfHeader.bytesRead();
    List<Integer> columnsType = new ArrayList<>();
    while(remaining > 0)
    {
      var varInt = Cell.from(buffer);
      columnsType.add((int)varInt.value());
      remaining -= varInt.bytesRead();
    }

    List<String> values = new ArrayList<>();
    for (var colType : columnsType)
    {
      switch (colType) {
        case 0:
          values.add("NULL");
          break;
        case 1:
          values.add(String.valueOf(buffer.get()));
          break;
        case 2:
          values.add(String.valueOf(buffer.getShort()));
          break;
        case 3:
          throw new RuntimeException("not implemented");
        case 4:
          values.add(String.valueOf(buffer.getInt()));
          break;
        case 5:
          throw new RuntimeException("not implemented");
        case 6:
          values.add(String.valueOf(buffer.getLong()));
          break;
        case 7:
          values.add(String.valueOf(buffer.getFloat()));
          break;
        case 8:
          values.add("0");
          break;
        case 9:
          values.add("1");
          break;
        default: {
          int contentSize = 0;
          if (colType >= 12 && colType % 2 == 0) {
            contentSize = (colType - 12) / 2;
          }
          if (colType >= 13 && colType % 2 == 1) {
            contentSize = (colType - 13) / 2;
          }
          if (contentSize > 0) {
            byte[] contents = new byte[contentSize];
            buffer.get(contents);
            values.add(new String(contents));
          } else {
            values.add("");
          }
        }
      }
    }
    return values;
  }
}