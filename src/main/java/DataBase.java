import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
public class DataBase {
    ByteBuffer fileContents;
    String databaseFilePath;
    public DataBase(String databaseFilePath) {
        this.databaseFilePath = databaseFilePath;
    }
    public void load() throws IOException {
        fileContents =
                ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
                        .order(ByteOrder.BIG_ENDIAN);
    }
    public record DBInfo(int pageSize, int numberOfTables) {}
    public DBInfo dbInfo() {
        // The page size is stored at the 16th byte offset, using 2 bytes in
        // big-endian order.
        // '& 0xFFFF' is used to convert the signed short to an unsigned int.
        int pageSize = fileContents.position(16).getShort() & 0xFFFF;
        ByteBuffer firstPage = fileContents.position(100);
        var pageHeader = BtreePageHeader.getHeader(firstPage);
        // leaf b-tree page
        assert pageHeader.pageType == 0x0d;
        return new DBInfo(pageSize, pageHeader.cellCounts);
    }
    public void printTableNames() throws IOException {
        Page result = getFirstPage();
        List<Row> records = new ArrayList<>();
        for (int i = 0; i < result.pageHeader().cellCounts; ++i) {
            records.add(getCellContents(result.contents()));
        }
        Collections.reverse(records);
        System.out.println(String.join(
                " ",
                records.stream().map(rec -> (String)rec.columnData.get(2)).toList()));
    }
    public int countRows(String table) throws IOException {
        Page firstPage = getFirstPage();
        byte rootPageIndex = 0;
        for (int i = 0; i < firstPage.pageHeader().cellCounts; ++i) {
            var cellContent = getCellContents(firstPage.contents);
            if (Objects.equals((String)cellContent.columnData.get(2), table)) {
                rootPageIndex = (byte)cellContent.columnData.get(3);
                break;
            }
        }
        return getNthPage(rootPageIndex).pageHeader.cellCounts;
    }
    Row getCellContents(ByteBuffer cellArray) {
        // varint
        VarInt bytesOfPayload = Cell.from(cellArray);
        // varint
        VarInt rowId = Cell.from(cellArray);
        // payload
        byte[] payload = new byte[(int)bytesOfPayload.value()];
        cellArray.get(payload);
        ByteBuffer recordBuf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        return getRowData(recordBuf);
    }
    public record Page(ByteBuffer contents, BtreePageHeader pageHeader) {}
    private Page getFirstPage() throws IOException {
        ByteBuffer firstPage = fileContents.position(100);
        var pageHeader = BtreePageHeader.getHeader(firstPage);
        firstPage.position(pageHeader.getGetStartOfCellContentArea());
        return new Page(firstPage, pageHeader);
    }
    private Page getNthPage(int n) {
        int pageSize = fileContents.position(16).getShort() & 0xFFFF;
        int firstPageOffset = n == 1 ? 100 : 0;
        ByteBuffer page =
                fileContents.position((n - 1) * pageSize + firstPageOffset);
        var pageHeader = BtreePageHeader.getHeader(page);
        return new Page(page, pageHeader);
    }
    record Row(List<Integer> columnTypes, List<Object> columnData) {}
    Object get(Row r, int n) {
        assert n >= 0 && n <= r.columnTypes.size();
        int colType = r.columnTypes.get(n);
        return switch (colType) {
            case 0 -> null;
            case 3, 5 -> throw new RuntimeException("not implemented");
            case 8 -> 0;
            case 9 -> 1;
            default -> r.columnData.get(n);
        };
    }
    Row getRowData(ByteBuffer buffer)
    {
        var sizeOfHeader = Cell.from(buffer);
        long remaining = sizeOfHeader.value() - sizeOfHeader.bytesRead();
        List<Integer> columnsType = new ArrayList<>();

        while (remaining > 0)
        {
            var varInt = Cell.from(buffer);
            columnsType.add((int)varInt.value());
            remaining -= varInt.bytesRead();
        }

        List<Object> values = new ArrayList<>();

        for (var colType : columnsType)
        {
            switch (colType) {
                case 0:
                    values.add(null);
                    break;
                case 1:
                    values.add(buffer.get());
                    break;
                case 2:
                    values.add(buffer.getShort());
                    break;
                case 3:
                    byte[] bytes =
                            new byte[] {buffer.get(), buffer.get(), buffer.get()};
                    // convert bytes to int
                    int intValue = 0;
                    for (byte b : bytes) {
                        intValue = (intValue << 8) + (b & 0xFF);
                    }
                    values.add(intValue);
                    break;
                case 4:
                    values.add(buffer.getInt());
                    break;
                case 5:
                    throw new RuntimeException("not implemented");
                case 6:
                    values.add(buffer.getLong());
                    break;
                case 7:
                    values.add(buffer.getFloat());
                    break;
                case 8:
                    values.add(0);
                    break;
                case 9:
                    values.add(1);
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
        return new Row(columnsType, values);
    }
}