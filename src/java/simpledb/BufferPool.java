package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private HashMap<PageId, Page> myPages;
    private int maxPages;
    private ArrayList<PageId> myQueue;
    private theLock myLock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxPages = numPages;
        myPages = new HashMap<PageId, Page>();
        myQueue = new ArrayList<PageId>();
        myLock = new theLock();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        boolean freeLock = myLock.getLock(pid, tid, perm);
        while (!freeLock) {
            freeLock = myLock.getLock(pid, tid, perm);
        }
        if (myPages.containsKey(pid)) {

            if (this.myQueue.remove(pid)) {
                this.myQueue.add(pid);
            }

            return myPages.get(pid);
        } else {
            Page retVal = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);

            if (myPages.size() > maxPages) {
                this.evictPage();
            } else {
                myPages.put(pid, retVal);
                this.myQueue.add(pid);
            }

            return retVal;
        }
    }

    public class theLock {

        private Hashtable<PageId, ArrayList<TransactionId>> pageToTransactionShared;
        private Hashtable<PageId, TransactionId> pageToTransactionExclusive;
        private Hashtable<TransactionId, ArrayList<PageId>> transactionToPageShared;
        private Hashtable<TransactionId, ArrayList<PageId>> transactionToPageExclusive;

        public theLock() {
            pageToTransactionExclusive = new Hashtable<PageId, TransactionId>();
            pageToTransactionShared = new Hashtable<PageId, ArrayList<TransactionId>>();
            transactionToPageShared = new Hashtable<TransactionId, ArrayList<PageId>>();
            transactionToPageExclusive = new Hashtable<TransactionId, ArrayList<PageId>>();
        }

        public synchronized void releaseAllLocks(TransactionId t) {
            for (PageId pid : pageToTransactionShared.keySet()) {
                ArrayList<TransactionId> sharedTransactions = pageToTransactionShared.get(pid);
                if (sharedTransactions != null) {
                    if (sharedTransactions.contains(t)) {
                        sharedTransactions.remove(t);
                    }
                    pageToTransactionShared.put(pid, sharedTransactions);
                }
            }
            ArrayList<PageId> pagesToRemove = new ArrayList<PageId>();
            for (PageId pid : pageToTransactionExclusive.keySet()) {
                TransactionId t1 = pageToTransactionExclusive.get(pid);
                if (t1 != null && t1.equals(t)) {
                    pagesToRemove.add(pid);
                }
            }
            transactionToPageExclusive.remove(t);
            for (PageId p : pagesToRemove) {
                pageToTransactionExclusive.remove(p);
            }
        }

        public synchronized boolean getLock(PageId p, TransactionId t, Permissions perm) {

            ArrayList<TransactionId> sharedT = pageToTransactionShared.get(p);
            TransactionId exclusiveT = pageToTransactionExclusive.get(p);

            boolean isValid = true;
            boolean usedSharedLock = false;
            boolean usedExclusiveLock = false;

            /** Checks to see if transactions have a shared lock on this page. */
            if (sharedT != null) {
                usedSharedLock = true;
            }

            /** Checks to see if a transaction has an exclusive lock on this page. */
            if (exclusiveT != null) {
                usedExclusiveLock = true;
            }

            if (perm.equals(Permissions.READ_WRITE)) {


                /** If transactions have a shared lock on this page. */
                if (usedSharedLock) {

                    /** Can't get exclusive lock because more than one transaction
                    /*  has control of the shared lock .*/
                    if (sharedT.size() > 1) {
                        isValid = false;
                        return isValid;
                    }

                    /** The other edge case is that only one transaction has a shared
                    /*  on this page. However, we have to make sure that said tranaction
                    /*  is actually OUR transaction. If not, BOOT. */
                    if (sharedT.size() == 1 && !sharedT.contains(t)) {
                        isValid = false;
                        return isValid;
                    }
                }

                /** If a transaction does have an exclusive lock, make sure that
                /*  that transaction is OUR transaction. If not, BOOT. */
                if (usedExclusiveLock && !exclusiveT.equals(t)) {
                    isValid = false;
                    return isValid;
                } else { /** AKA we are clear! */
                    isValid = true;

                    pageToTransactionExclusive.put(p, t);
                    ArrayList<PageId> transactionToPageArrayList = transactionToPageExclusive.get(t);
                    
                    if (transactionToPageArrayList == null) {
                        transactionToPageArrayList = new ArrayList<PageId>();
                    }

                    if (!transactionToPageArrayList.contains(p)) {
                        transactionToPageArrayList.add(p);
                    }

                    transactionToPageExclusive.put(t, transactionToPageArrayList);
                    return isValid;
                }

            } else {

                if (!usedExclusiveLock || exclusiveT.equals(t)) {
                    if (sharedT == null) {
                        sharedT = new ArrayList<TransactionId>();
                    }

                    if (!sharedT.contains(t)) {
                        sharedT.add(t);
                    }
                    pageToTransactionShared.put(p, sharedT);

                    ArrayList<PageId> transactionToPageArrayList = transactionToPageShared.get(t);
                    if (transactionToPageArrayList == null) {
                        transactionToPageArrayList = new ArrayList<PageId>();
                    }

                    if (!transactionToPageArrayList.contains(p)) {
                        transactionToPageArrayList.add(p);
                    }

                    transactionToPageShared.put(t, transactionToPageArrayList);
                    isValid = true;
                    return isValid;

                } else {
                    isValid = false;
                    return isValid;
                }
            }
        }


        public synchronized void releasePage(TransactionId t, PageId p) {

            ArrayList<PageId> exclusivePages = transactionToPageExclusive.get(t);
            if (exclusivePages != null) {
                exclusivePages.remove(p);
                transactionToPageShared.put(t, exclusivePages);
            }

            ArrayList<TransactionId> sharedTransactions = pageToTransactionShared.get(p);
            if (sharedTransactions != null) {
                sharedTransactions.remove(t);
                pageToTransactionShared.put(p, sharedTransactions);
            }


            ArrayList<PageId> sharedPages = transactionToPageShared.get(t);
            if (sharedPages != null) {
                sharedPages.remove(p);
                transactionToPageShared.put(t, sharedPages);
            }

            pageToTransactionExclusive.remove(p);
        }

        public synchronized boolean holdsLock(TransactionId t, PageId p) {
            TransactionId exclusiveT = pageToTransactionExclusive.get(p);
            if (exclusiveT != null && exclusiveT.equals(t)) {
                return true;
            }


            ArrayList<TransactionId> transactionPages = pageToTransactionShared.get(p);
            if (transactionPages != null && transactionPages.contains(t)) {
                return true;
            }

            return false;
        }


    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        myLock.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        boolean retVal;
        retVal = myLock.holdsLock(tid, p);
        return retVal;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        Collection<Page> pages = myPages.values();
        if (commit) {
            for (Page p : pages) {
                if (p.isDirty() == null) {
                    p.setBeforeImage();
                } else {
                    if (p.isDirty().equals(tid)) {
                        flushPage(p.getId());
                    }
                }
            }
        } else {
            for (Page p : pages) {  
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                    PageId pid = p.getId();
                    Page beforeImage = p.getBeforeImage();
                    myPages.put(pid, beforeImage);
                }
            }
        }
        myLock.releaseAllLocks(tid);

        
    }
    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1

        HeapFile myHeapFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> insertTuple = myHeapFile.insertTuple(tid, t);
        for (Page p : insertTuple) {
            p.markDirty(true, tid);
            PageId pid = p.getId();
            if (!this.myPages.containsKey(pid)) {
                this.myPages.put(pid, p); /** LRU stuff */
                if (this.myPages.size() > maxPages) {
                    this.evictPage();
                }
                this.myQueue.add(pid);
            } else {
                if (this.myQueue.remove(pid)) {
                    this.myQueue.add(pid);
                }
            }
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
            int tid2 = t.getRecordId().getPageId().getTableId();
            HeapFile myHeapFile = (HeapFile) Database.getCatalog().getDbFile(tid2);
            Page modifiedPage = myHeapFile.deleteTuple(tid, t);
            modifiedPage.markDirty(true, tid);

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        for (PageId p : this.myPages.keySet()) {
            flushPage(p);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
        if (this.myPages.containsKey(pid)) {
            this.myPages.remove(pid);
            if (this.myQueue.contains(pid)) {
                this.myQueue.remove(pid);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        Page thePage = this.myPages.get(pid);
        if (thePage.isDirty() != null) {
            Database.getCatalog().getDbFile(pid.getTableId()).writePage(thePage);   
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1

        boolean isAllDirty = true;
        boolean isEvicted = false;
        Collection<Page> allPagesValue = myPages.values();
        while (isAllDirty) {
            for (Page p : allPagesValue) {
                if (p.isDirty() == null) {
                    isAllDirty = false;
                    break;
                }
            }
        
            if (isAllDirty) {
                throw new DbException("all dirty.");
            }
        }
        
        while (!isEvicted) {
            for (int i = 0; i < myQueue.size(); i++) {
                PageId pid = myQueue.get(i);
                if (myPages.get(pid).isDirty() == null) {
                    try {
                        flushPage(pid);
                        myPages.remove(pid);
                        myQueue.remove(i);
                        isEvicted = true;
                        break;                    
                    } catch (IOException e) {
                        System.out.println("Exception Thrown:" + e.getMessage());
                    }

                }
            }
                   
        }
    }

}

