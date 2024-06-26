import java.nio.ByteBuffer;
import java.util.List;
public class BtreePage {
    BtreePageHeader btreePageHeader;
    short[] cellPointerArray;
    byte[] pageContents;
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
    public static BtreePage readPage(ByteBuffer fileBuffer, int pageNumber) {
        int pageSize = fileBuffer.position(16).getShort() & 0xFFFF;
        int firstPageOffset = pageNumber == 1 ? 100 : 0;
        int pageOffset = (pageNumber - 1) * pageSize + firstPageOffset;
        fileBuffer.position(pageOffset);
        var header = BtreePageHeader.getHeader(fileBuffer);
        var cellPointerArray = new short[header.cellCounts];
        for (int i = 0; i < header.cellCounts; ++i) {
            cellPointerArray[i] = fileBuffer.getShort();
        }
        fileBuffer.position((pageNumber - 1) * pageSize);
        byte[] pageContents = new byte[pageSize];
        fileBuffer.get(pageContents);
        return new BtreePage(header, cellPointerArray, pageContents);
    }
}