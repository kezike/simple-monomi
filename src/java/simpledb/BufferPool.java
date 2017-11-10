package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPgs;
    private ConcurrentHashMap<PageId, Page> idToPage;
    private HashSet<PageId> pids;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPgs = numPages;
        this.idToPage = new ConcurrentHashMap<PageId, Page>();
        this.pids = new HashSet<PageId>();
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (perm.equals(Permissions.READ_ONLY)) {
          this.lockManager.acquireSLock(tid, pid);
        } else if (perm.equals(Permissions.READ_WRITE)) {
          this.lockManager.acquireXLock(tid, pid);
        }
        Catalog catalog = Database.getCatalog();
        Page page = this.idToPage.get(pid);
        if (page == null) {
          if (this.idToPage.size() == this.numPgs) {
            this.evictPage();
          }
          int tableId = pid.getTableId();
          DbFile table = catalog.getDatabaseFile(tableId);
          page = table.readPage(pid);
          this.idToPage.put(pid, page);
          this.pids.add(pid);
        }
        return page;
    }

    /**
     * Acquire write access on page
     *
     * @param tid the ID of the transaction requesting write access
     * @param pid the ID of the page of interest
    */
    public void acquireXLock(TransactionId tid, PageId pid) {
        this.lockManager.acquireXLock(tid, pid);
    }

    /**
     * Acquire read access on page
     *
     * @param tid the ID of the transaction requesting read access
     * @param pid the ID of the page of interest
    */
    public void acquireSLock(TransactionId tid, PageId pid) {
        this.lockManager.acquireSLock(tid, pid);
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(tid, pid);
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
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> xPids = this.lockManager.getXLockStatus(tid);
        HashSet<PageId> sPids = this.lockManager.getSLockStatus(tid);
        Catalog catalog = Database.getCatalog();
        if (commit) {
          if (xPids != null) {
            synchronized(xPids) {
              for (PageId pid : xPids) {
                this.flushPage(pid);
                this.releasePage(tid, pid);
              }
            }
          }
          if (sPids != null) {
            synchronized(sPids) {
              for (PageId pid : sPids) {
                this.flushPage(pid);
                this.releasePage(tid, pid);
              }
            }
          }
        } else {
          if (xPids != null) {
            synchronized(xPids) {
              for (PageId pid : xPids) {
                int tableId = pid.getTableId();
                DbFile table = catalog.getDatabaseFile(tableId);
                Page page = table.readPage(pid);
                this.idToPage.put(pid, page);
                this.pids.add(pid);
              }
            }
          }
          if (sPids != null) {
            synchronized(sPids) {
              for (PageId pid : sPids) {
                int tableId = pid.getTableId();
                DbFile table = catalog.getDatabaseFile(tableId);
                Page page = table.readPage(pid);
                this.idToPage.put(pid, page);
                this.pids.add(pid);
              }
            }
          }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog catalog = Database.getCatalog();
        DbFile table = catalog.getDatabaseFile(tableId);
        ArrayList<Page> pagesAffected = table.insertTuple(tid, t);
        for (Page pageAffected : pagesAffected) {
          pageAffected.markDirty(true, tid);
          PageId pAffId = pageAffected.getId();
          this.idToPage.put(pAffId, pageAffected);
          this.pids.remove(pAffId);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        int tableId = pid.getTableId();
        Catalog catalog = Database.getCatalog();
        DbFile table = catalog.getDatabaseFile(tableId);
        ArrayList<Page> pagesAffected = table.deleteTuple(tid, t);
        for (Page pageAffected : pagesAffected) {
          pageAffected.markDirty(true, tid);
          PageId pAffId = pageAffected.getId();
          this.idToPage.put(pAffId, pageAffected);
          this.pids.remove(pAffId);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	Set<PageId> pids = this.idToPage.keySet();
    	for (PageId pid : pids) {
          this.flushPage(pid);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.idToPage.remove(pid);
        this.pids.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int tableId = pid.getTableId();
    	Catalog catalog = Database.getCatalog();
    	DbFile table = catalog.getDatabaseFile(tableId);
    	Page page = this.idToPage.get(pid);
    	TransactionId tid = new TransactionId();
    	if (page != null) {
    	  table.writePage(page);
          page.markDirty(false, tid);
          this.pids.add(pid);
          this.idToPage.put(pid, page);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	// TransactionId tid;
        // PageId pid;
        // Page page;
        // Iterator<PageId> pidIter = this.pids.iterator();
        /*while (true) {
          try {
            tid = new TransactionId();
            pid = pidIter.next();
            System.out.println("Getting Page...");
            Page page = this.getPage(tid, pid, Permissions.READ_WRITE);
            System.out.println("Got Page!");
            TransactionId dirty = page.isDirty();
            if (dirty != null) {
              break;
            }
            this.releasePage(tid, pid);
          } catch (NoSuchElementException nseExn) {
            throw new DbException("No clean pages available for eviction!");
          } catch (TransactionAbortedException txnAbExn) {
            return;
          }
        }*/
        /*Set<Map.Entry<PageId, Page>> entrySet = this.idToPage.entrySet();
        for (Map.Entry<PageId, Page> entry : entrySet) {
          PageId pid = entry.getKey();
          Page page = entry.getValue();
          TransactionId dirty = page.isDirty();
          if (dirty == null) {
            // try {
              // this.flushPage(pid);
              this.discardPage(pid);
            // } catch (IOException ioExn) {
            // }
            return;
          }
        }
        throw new DbException("No clean pages available for eviction!");*/
        try {
          PageId pid = this.pids.iterator().next();
          // this.flushPage(pid);
          this.discardPage(pid);
        } catch (NoSuchElementException nseExn) {
          throw new DbException("No clean pages available for eviction!");
        } // catch (IOException ioExn) {
        // }
    }
}

