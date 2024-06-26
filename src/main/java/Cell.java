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
        return switch (type) {
            case 0x0d -> { // leaf table
                VarInt bytesOfPayload = from(buffer);
                VarInt rowId = from(buffer);
                byte[] payload = new byte[(int) bytesOfPayload.value()];
                buffer.get(payload);
                yield new Cell(type, bytesOfPayload, rowId, payload);
            }
            case 0x05 -> { // interior table
                int leftChildPointer = buffer.getInt();
                VarInt rowId = from(buffer);
                yield new Cell(type, leftChildPointer, rowId);
            }
            case 0x0a -> { // leaf index
                VarInt bytesOfPayload = from(buffer);
                byte[] payload = new byte[(int) bytesOfPayload.value()];
                buffer.get(payload);
                yield new Cell(type, bytesOfPayload, payload);
            }
            case 0x02 -> { // interior index
                int leftChildPointer = buffer.getInt();
                VarInt bytesOfPayload = from(buffer);
                byte[] payload = new byte[(int) bytesOfPayload.value()];
                buffer.get(payload);
                Cell cell = new Cell(type, bytesOfPayload, payload);
                cell.leftChildPointer = leftChildPointer;
                yield cell;
            }
            default -> throw new UnrecognizedCellTypeException("Unrecognized cell type: " + type);
        };
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

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public VarInt getBytesOfPayload() {
        return bytesOfPayload;
    }

    public void setBytesOfPayload(VarInt bytesOfPayload) {
        this.bytesOfPayload = bytesOfPayload;
    }

    public VarInt getRowId() {
        return rowId;
    }

    public void setRowId(VarInt rowId) {
        this.rowId = rowId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public int getLeftChildPointer() {
        return leftChildPointer;
    }

    public void setLeftChildPointer(int leftChildPointer) {
        this.leftChildPointer = leftChildPointer;
    }

    static class UnrecognizedCellTypeException extends RuntimeException {
        public UnrecognizedCellTypeException(String message) {
            super(message);
        }
    }
}
