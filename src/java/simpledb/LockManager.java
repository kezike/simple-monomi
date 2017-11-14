package simpledb;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

/** Class for maintaining lock ownership information
*/
public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> xLocksPidToTid;
    private ConcurrentHashMap<PageId, Set<TransactionId>> sLocksPidToTid;
    private ConcurrentHashMap<TransactionId, Set<PageId>> xLocksTidToPid;
    private ConcurrentHashMap<TransactionId, Set<PageId>> sLocksTidToPid;

    public LockManager() {
        this.xLocksPidToTid = new ConcurrentHashMap<PageId, TransactionId>();
        this.sLocksPidToTid = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        this.xLocksTidToPid = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        this.sLocksTidToPid = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    }

    public boolean acquireXLock(TransactionId tid, PageId pid, long dlTimeOut) {
        TransactionId xTid;
        Set<TransactionId> sTids;
        long start = System.currentTimeMillis();
        while (true) {
          if (System.currentTimeMillis() - start > dlTimeOut) {
            return false;
          }
          synchronized (this) {
            boolean xLockIsFree = true;
            boolean sLockIsFree = true;
            xTid = this.xLocksPidToTid.get(pid);
            if (xTid != null) {
              if (!tid.equals(xTid)) {
                xLockIsFree = false;
              }
            }
            sTids = this.sLocksPidToTid.get(pid);
            if (sTids != null) {
              if (!sTids.contains(tid)) {
                sLockIsFree = false;
              } else {
                if (sTids.size() == 1) {
                  sLockIsFree = true;
                } else {
                  sLockIsFree = false;
                }
              }
            }
            if (xLockIsFree && sLockIsFree) {
              Set<PageId> xPids = this.xLocksTidToPid.get(tid);
              if (xPids == null) {
                xPids = Collections.synchronizedSet(new HashSet<PageId>());
              }
              synchronized (xPids) {
                xPids.add(pid);
                this.xLocksTidToPid.put(tid, xPids);
              }
              this.xLocksPidToTid.put(pid, tid);
              return true;
            }
          }
          /*try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
          }*/
        }
    }
    
    public boolean acquireSLock(TransactionId tid, PageId pid, long dlTimeOut) {
        TransactionId xTid;
        long start = System.currentTimeMillis();
        while (true) {
          if (System.currentTimeMillis() - start > dlTimeOut) {
            return false;
          }
          synchronized (this) {
            boolean xLockIsFree = true;
            xTid = this.xLocksPidToTid.get(pid);
            if (xTid != null) {
              if (!tid.equals(xTid)) {
                xLockIsFree = false;
              }
            }
            if (xLockIsFree) {
              Set<PageId> sPids = this.sLocksTidToPid.get(tid);
              if (sPids == null) {
                sPids = Collections.synchronizedSet(new HashSet<PageId>());
              }
              synchronized (sPids) {
                sPids.add(pid);
                this.sLocksTidToPid.put(tid, sPids);
              }
              Set<TransactionId> sTids = this.sLocksPidToTid.get(pid);
              if (sTids == null) {
                sTids = Collections.synchronizedSet(new HashSet<TransactionId>());
              }
              synchronized (sTids) {
                sTids.add(tid);
                this.sLocksPidToTid.put(pid, sTids);
              }
              return true;
            }
          }
          /*try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
          }*/
        }
    }

    public synchronized boolean holdsXLock(TransactionId tid, PageId pid) {
    	TransactionId xTid = this.xLocksPidToTid.get(pid);
        boolean holdsXLock = (xTid != null);
        return holdsXLock;
    }

    public synchronized boolean holdsSLock(TransactionId tid, PageId pid) {
    	Set<TransactionId> sTids = this.sLocksPidToTid.get(pid);
        boolean holdsSLock = false;
        if (sTids != null) {
          if (sTids.contains(tid)) {
            holdsSLock = true;
          }
        }
        return holdsSLock;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        boolean holdsLock = holdsXLock(tid, pid) || holdsSLock(tid, pid);
        return holdsLock;
    }

    public void releasePage(TransactionId tid, PageId pid) {
        synchronized (this) {
          TransactionId xTid = this.xLocksPidToTid.get(pid);
          Set<TransactionId> sTids = this.sLocksPidToTid.get(pid);
          Set<PageId> xPids = this.xLocksTidToPid.get(tid);
          Set<PageId> sPids = this.sLocksTidToPid.get(tid);
          if (tid.equals(xTid)) {
            this.xLocksPidToTid.remove(pid);
          }
          if (sTids != null) {
            sTids.remove(tid);
            if (sTids.isEmpty()) {
              this.sLocksPidToTid.remove(pid);
            }
          }
          if (sPids != null) {
            sPids.remove(pid);
            if (sPids.isEmpty()) {
              this.sLocksTidToPid.remove(tid);
            }
          }
          if (xPids != null) {
            xPids.remove(pid);
            if (xPids.isEmpty()) {
              this.xLocksTidToPid.remove(tid);
            }
          }
        }
    }
    
    public Set<PageId> getXLockStatus(TransactionId tid) {
        return this.xLocksTidToPid.get(tid);
    }
    
    public Set<PageId> getSLockStatus(TransactionId tid) {
        return this.sLocksTidToPid.get(tid);
    }
}
