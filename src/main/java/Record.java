import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
public class Record {
    VarInt totalBytesInHeader;
    List<Integer> serialTypes;
    List<Object> values;
    public Record(VarInt totalBytesInHeader, List<Integer> serialTypes,
                  List<Object> values) {
        this.totalBytesInHeader = totalBytesInHeader;
        this.serialTypes = serialTypes;
        this.values = values;
    }
    public static Record readRecord(ByteBuffer buffer) {
        var sizeOfHeader = Cell.from(buffer);
        long remaining = sizeOfHeader.value() - sizeOfHeader.bytesRead();
        List<Integer> columnsType = new ArrayList<>();
        while (remaining > 0) {
            var varInt = Cell.from(buffer);
            columnsType.add((int)varInt.value());
            remaining -= varInt.bytesRead();
        }
        List<Object> values = new ArrayList<>();
        for (var colType : columnsType) {
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
                    byte[] bytes = new byte[] {buffer.get(), buffer.get(), buffer.get()};
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
        return new Record(sizeOfHeader, columnsType, values);
    }
    public VarInt getTotalBytesInHeader() { return totalBytesInHeader; }
    public void setTotalBytesInHeader(VarInt totalBytesInHeader) {
        this.totalBytesInHeader = totalBytesInHeader;
    }
    public List<Integer> getSerialTypes() { return serialTypes; }
    public void setSerialTypes(List<Integer> serialTypes) {
        this.serialTypes = serialTypes;
    }
    public List<Object> getValues() { return values; }
    public void setValues(List<Object> values) { this.values = values; }
    public Object getValue(int n) {
        assert n >= 0 && n <= this.serialTypes.size();
        int colType = this.serialTypes.get(n);
        return switch (colType) {
        case 0 -> null;
        case 5 -> throw new RuntimeException("not implemented");
        case 8 -> 0;
        case 9 -> 1;
        default -> values.get(n);
    };
}
}