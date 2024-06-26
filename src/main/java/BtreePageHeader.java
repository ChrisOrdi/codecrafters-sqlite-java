import java.nio.ByteBuffer;
public class BtreePageHeader {
    byte pageType;
    short freeBlocks;
    short cellCounts;
    short getStartOfCellContentArea;
    byte numberOfFragmentedFreeBytes;
    int rightMostPointer;
    public BtreePageHeader(byte pageType, short freeBlocks, short cellCounts,
                           short getStartOfCellContentArea,
                           byte numberOfFragmentedFreeBytes,
                           int rightMostPointer) {
        this.pageType = pageType;
        this.freeBlocks = freeBlocks;
        this.cellCounts = cellCounts;
        this.getStartOfCellContentArea = getStartOfCellContentArea;
        this.numberOfFragmentedFreeBytes = numberOfFragmentedFreeBytes;
        this.rightMostPointer = rightMostPointer;
    }
    static BtreePageHeader getHeader(ByteBuffer page) {
        byte pageType = page.get();
        short freeBlocks = page.getShort();
        short cellCounts = page.getShort();
        short startOfCellContentArea = page.getShort();
        byte numberOfFragmentedBytes = page.get();
        int rightMostPointer = page.getInt();
        return new BtreePageHeader(pageType, freeBlocks, cellCounts,
                startOfCellContentArea, numberOfFragmentedBytes,
                rightMostPointer);
    }
    public byte getPageType() { return pageType; }
    public void setPageType(byte pageType) { this.pageType = pageType; }
    public short getFreeBlocks() { return freeBlocks; }
    public void setFreeBlocks(short freeBlocks) { this.freeBlocks = freeBlocks; }
    public short getCellCounts() { return cellCounts; }
    public void setCellCounts(short cellCounts) { this.cellCounts = cellCounts; }
    public short getGetStartOfCellContentArea() {
        return getStartOfCellContentArea;
    }
    public void setGetStartOfCellContentArea(short getStartOfCellContentArea) {
        this.getStartOfCellContentArea = getStartOfCellContentArea;
    }
    public byte getNumberOfFragmentedFreeBytes() {
        return numberOfFragmentedFreeBytes;
    }
    public void setNumberOfFragmentedFreeBytes(byte numberOfFragmentedFreeBytes) {
        this.numberOfFragmentedFreeBytes = numberOfFragmentedFreeBytes;
    }
    public int getRightMostPointer() { return rightMostPointer; }
    public void setRightMostPointer(int rightMostPointer) {
        this.rightMostPointer = rightMostPointer;
    }
    @Override
    public String toString() {
        return "BtreePageHeader{"
                + "pageType=" + pageType + ", freeBlocks=" + freeBlocks +
                ", cellCounts=" + cellCounts +
                ", getStartOfCellContentArea=" + getStartOfCellContentArea +
                ", numberOfFragmentedFreeBytes=" + numberOfFragmentedFreeBytes +
                ", rightMostPointer=" + rightMostPointer + '}';
    }
}