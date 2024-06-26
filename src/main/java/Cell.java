import java.nio.ByteBuffer;
public class Cell {
    byte type;
    VarInt bytesOfPayload;
    VarInt rowId;
    byte[] payload;
    // for B-Tree Interior pages
    int leftChildPointer;
    int firstPageOfOverflow;
    public Cell(byte type, VarInt bytesOfPayload, VarInt rowId, byte[] payload) {
        this.type = type;
        this.bytesOfPayload = bytesOfPayload;
        this.rowId = rowId;
        this.payload = payload;
    }
    public Cell(byte type, int leftChildPointer, VarInt rowId) {
        this.type = type;
        this.leftChildPointer = leftChildPointer;
        this.rowId = rowId;
    }
    public Cell(byte type, VarInt bytesOfPayload, byte[] payload) {
        this.type = type;
        this.bytesOfPayload = bytesOfPayload;
        this.payload = payload;
    }
    public static Cell readCell(ByteBuffer buffer, byte type) {
        if (type == 0x0d) {
            // leaf table
            var bytesOfPayload = from(buffer);
            var rowId = from(buffer);
            byte[] payload = new byte[(int)bytesOfPayload.value()];
            buffer.get(payload);
            return new Cell(type, bytesOfPayload, rowId, payload);
        } else if (type == 0x05) {
            // interior table
            int leftChildPointer = buffer.getInt();
            VarInt rowId = from(buffer);
            return new Cell(type, leftChildPointer, rowId);
        } else if (type == 0x0a) {
            // leaf index
            var bytesOfPayload = from(buffer);
            byte[] payload = new byte[(int)bytesOfPayload.value()];
            buffer.get(payload);
            return new Cell(type, bytesOfPayload, payload);
        } else if (type == 0x02) {
            // interior index
            int leftChildPointer = buffer.getInt();
            var bytesOfPayload = from(buffer);
            byte[] payload = new byte[(int)bytesOfPayload.value()];
            buffer.get(payload);
            Cell cell = new Cell(type, bytesOfPayload, payload);
            cell.leftChildPointer = leftChildPointer;
            return cell;
        } else {
            throw new RuntimeException("unrecognized cell type: " + type);
        }
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
    public int getLeftChildPointer() { return leftChildPointer; }
    public void setLeftChildPointer(int leftChildPointer) {
        this.leftChildPointer = leftChildPointer;
    }
}