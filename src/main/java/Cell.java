import java.nio.ByteBuffer;
public class Cell {
    byte type;
    VarInt bytesOfPayload;
    VarInt rowId;
    byte[] payload;
    public Cell(byte type, VarInt bytesOfPayload, VarInt rowId, byte[] payload) {
        this.type = type;
        this.bytesOfPayload = bytesOfPayload;
        this.rowId = rowId;
        this.payload = payload;
    }
    public static Cell readCell(ByteBuffer buffer, byte type) {
        var bytesOfPayload = from(buffer);
        var rowId = from(buffer);
        byte[] payload = new byte[(int)bytesOfPayload.value()];
        buffer.get(payload);
        return new Cell(type, bytesOfPayload, rowId, payload);
    }
    public static VarInt from(ByteBuffer buff) {
        long result = 0L;
        int bytesRead = 0;
        for (int i = 0; i < 9; ++i) {
            byte b = buff.get();
            bytesRead++;
            result = (result << 7) + (b & 0x7f);
            if (((b >> 7) & 1) == 0) {
                break;
            }
        }
        return new VarInt(bytesRead, result);
    }
    public byte getType() { return type; }
    public void setType(byte type) { this.type = type; }
    public VarInt getBytesOfPayload() { return bytesOfPayload; }
    public void setBytesOfPayload(VarInt bytesOfPayload) {
        this.bytesOfPayload = bytesOfPayload;
    }
    public VarInt getRowId() { return rowId; }
    public void setRowId(VarInt rowId) { this.rowId = rowId; }
    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}