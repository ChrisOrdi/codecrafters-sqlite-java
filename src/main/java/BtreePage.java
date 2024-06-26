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

        //    public static BtreePage readPage(ByteBuffer fileBuffer, int pageNumber){
        //        int pageSize = fileBuffer.position(16).getShort() & 0xFFFF;
        //        int firstPageOffset = pageNumber == 1 ? 100 : 0;
        //        int pageOffset = (pageNumber-1) * pageSize + firstPageOffset;
        //        fileBuffer.position(pageOffset);
        //        var header = BtreePageHeader.getHeader(fileBuffer);
        //        var cellPointerArray = new short[header.cellCounts];
        //        for(int i=0;i<header.cellCounts;++i){
        //            cellPointerArray[i] = fileBuffer.getShort();
        //        }
        //        fileBuffer.position((pageNumber-1)*pageSize);
        //        byte[] pageContents = new byte[pageSize];
        //        fileBuffer.get(pageContents);
        //        return new BtreePage(header, cellPointerArray, pageContents);
        //    }

        public static BtreePage readPage(RandomAccessFile file, int pageSize,
        int pageNumber) throws IOException {
            byte[] pageContents = new byte[pageSize];
            int pageOffset = (pageNumber - 1) * pageSize;
            file.seek(pageOffset);
            var filesRead = file.read(pageContents);
            assert filesRead == pageSize;
            ByteBuffer pageBuffer =
                    ByteBuffer.wrap(pageContents).order(ByteOrder.BIG_ENDIAN);
            if (pageNumber == 1) { // skip db header
                pageBuffer.position(100);
            }
            var header = BtreePageHeader.getHeader(pageBuffer);
            var cellPointerArray = new short[header.cellCounts];
            for (int i = 0; i < header.cellCounts; ++i) {
                cellPointerArray[i] = pageBuffer.getShort();
            }
            return new BtreePage(header, cellPointerArray, pageContents);
        }


        public void popCells() {
            //        System.out.printf("pageType: %d\n",
            //        this.btreePageHeader.pageType);
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