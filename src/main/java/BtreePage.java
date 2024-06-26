import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
public class BtreePage {
    BtreePageHeader btreePageHeader;
    short[] cellPointerArray;
    byte[] pageContents;
    Cell[] cellArray;
    Record[] records;
    public BtreePage(BtreePageHeader pageHeader, short[] cellPointerArray,
                     byte[] pageContents) {
        this.btreePageHeader = pageHeader;
        this.cellPointerArray = cellPointerArray;
        this.pageContents = pageContents;
    }
    public BtreePageHeader getBtreePageHeader() { return btreePageHeader; }
    public void setBtreePageHeader(BtreePageHeader btreePageHeader) {
        this.btreePageHeader = btreePageHeader;
    }
    public short[] getCellPointerArray() { return cellPointerArray; }
    public void setCellPointerArray(short[] cellPointerArray) {
        this.cellPointerArray = cellPointerArray;
    }
    public byte[] getPageContents() { return pageContents; }
    public void setPageContents(byte[] pageContents) {
        this.pageContents = pageContents;
    }

    public static BtreePage readPage(RandomAccessFile file, int pageSize, int pageNumber) throws IOException {
        byte[] pageContents = new byte[pageSize];
        int pageOffset = (pageNumber - 1) * pageSize;
        file.seek(pageOffset);
        int filesRead = file.read(pageContents);

        if (filesRead != pageSize) {
            throw new IOException("Failed to read the entire page. Expected: " + pageSize + " bytes, but read: " + filesRead + " bytes.");
        }

        ByteBuffer pageBuffer = ByteBuffer.wrap(pageContents).order(ByteOrder.BIG_ENDIAN);
        if (pageNumber == 1) { // skip db header
            pageBuffer.position(100);
        }

        BtreePageHeader header = BtreePageHeader.getHeader(pageBuffer);
        short[] cellPointerArray = new short[header.cellCounts];
        for (int i = 0; i < header.cellCounts; ++i) {
            cellPointerArray[i] = pageBuffer.getShort();
        }

        return new BtreePage(header, cellPointerArray, pageContents);
    }



    public void popCells() {
            this.cellArray = new Cell[this.cellPointerArray.length];
            if (this.btreePageHeader.pageType != 0x05) {
                this.records = new Record[this.cellPointerArray.length];
            }
            ByteBuffer pageBuffer =
                    ByteBuffer.wrap(this.pageContents).order(ByteOrder.BIG_ENDIAN);
            int i = 0;
            for (var cellPointer : cellPointerArray) {
                pageBuffer.position(cellPointer);
                var cell = Cell.readCell(pageBuffer, this.btreePageHeader.pageType);
                cellArray[i] = cell;
                if (this.btreePageHeader.pageType != 0x05) {
                    records[i] = Record.readRecord(
                            ByteBuffer.wrap(cell.payload).order(ByteOrder.BIG_ENDIAN));
                }
                i++;
            }
        }
    }