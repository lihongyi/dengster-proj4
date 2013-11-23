package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private TupleDesc myTupleDesc;
    private File myFile;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.myTupleDesc = td;
        this.myFile = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.myFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.myFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.myTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage retVal = null;
        try {
            FileInputStream f = new FileInputStream(this.myFile);
            int offset = BufferPool.PAGE_SIZE*pid.pageNumber();
            byte[] read_in = new byte[BufferPool.PAGE_SIZE];
            f.skip(offset);
            f.read(read_in);
            HeapPageId casted_pid = (HeapPageId) pid;
            retVal = new HeapPage(casted_pid, read_in);
        } catch (IOException e) {
            //who cares
        }
        return retVal;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1

        /** STILL NEED TO DO THIS SHIT */
        int pagina = page.getId().pageNumber();
        int offset = pagina * BufferPool.PAGE_SIZE;
        byte[] writeData = new byte[BufferPool.PAGE_SIZE];
        writeData = page.getPageData();
        RandomAccessFile f = new RandomAccessFile(this.myFile, "rw");
        f.seek(offset);
        f.write(writeData, 0, BufferPool.PAGE_SIZE);
        f.close();
        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int retVal = (int) Math.ceil(myFile.length()/BufferPool.PAGE_SIZE);
        return retVal;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bPool = Database.getBufferPool();
        ArrayList<Page> retVal = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage getPage = (HeapPage) bPool.getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (getPage.getNumEmptySlots() > 0) {
                getPage.insertTuple(t);
                retVal.add(getPage);
                return retVal;
            }
        }


        /** Or we append shit. */
        HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
        HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        /* Find free page and retrieve it. */

        /** WRITE THAT SHIT. */
        RandomAccessFile f = new RandomAccessFile(this.myFile, "rw");
        f.seek(this.numPages() * BufferPool.PAGE_SIZE);
        f.write(newPage.getPageData(), 0, BufferPool.PAGE_SIZE);
        f.close();
        // not necessary for proj1

        retVal.add(newPage);
        return retVal;
    }



    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        BufferPool bPool = Database.getBufferPool();
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) bPool.getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return page;
        // not necessary for proj1
    }

    // TOOK WAY TOO FRIGGEN LONG TO WRITE JESUS CHRIST
    public class InternalIterator implements DbFileIterator {
        
        public InternalIterator(HeapFile h, TransactionId tid) {
            this.myFile = h;
            this.transactionId = tid;
            this.openForSale = false;
        }
        
        private TransactionId transactionId;
        private HeapFile myFile;
        private int current_page;
        private boolean openForSale;
        private HeapPageId current_heapPageId;
        private HeapPage current_heapPage;
        private Iterator<Tuple> current_iterator;

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            boolean retVal = false;
            if (!openForSale) { return false; }
            if (current_iterator.hasNext()) {
                return true;
            }
            if (current_page < myFile.numPages()-1) {
                this.upThePage();
                retVal = current_iterator.hasNext();
            }
            return retVal;
        }

        public void upThePage() throws DbException, TransactionAbortedException {
            current_page++;
            current_heapPageId = new HeapPageId(myFile.getId(), current_page);
            current_heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, current_heapPageId, Permissions.READ_ONLY);
            current_iterator = current_heapPage.iterator();
        }
        
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) { return current_iterator.next(); } else { throw new NoSuchElementException("Sorry."); }
        }

        @Override
        public void close() {
            this.openForSale = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.current_page = 0;
            this.current_heapPageId = new HeapPageId(myFile.getId(), current_page);
            this.current_heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, current_heapPageId, Permissions.READ_ONLY);
            this.current_iterator = current_heapPage.iterator();
            this.openForSale = true;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new InternalIterator(this, tid);
    }

}

