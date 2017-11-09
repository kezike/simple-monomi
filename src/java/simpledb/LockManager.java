package simpledb;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.Set;

/** Class for maintaining lock ownership information
*/
public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> xLocksPidToTid;
    private ConcurrentHashMap<PageId, HashSet<TransactionId>> sLocksPidToTid;
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> xLocksTidToPid;
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> sLocksTidToPid;

    public LockManager() {
        this.xLocksPidToTid = new ConcurrentHashMap<PageId, TransactionId>();
        this.sLocksPidToTid = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
        this.xLocksTidToPid = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
        this.sLocksTidToPid = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
    }

    public void acquireXLock(TransactionId tid, PageId pid) {
        TransactionId xTid;
        HashSet<TransactionId> sTids;
        while (true) {
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
              }
            }
            if (xLockIsFree && sLockIsFree) {
              HashSet<PageId> xPids = this.xLocksTidToPid.get(tid);
              if (xPids == null) {
                xPids = new HashSet<PageId>();
              }
              xPids.add(pid);
              this.xLocksTidToPid.put(tid, xPids);
              this.xLocksPidToTid.put(pid, tid);
              break;
            }
          }
          /*try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
          }*/
        }
    }
    
    public void acquireSLock(TransactionId tid, PageId pid) {
        TransactionId xTid;
        while (true) {
          synchronized (this) {
            boolean xLockIsFree = true;
            xTid = this.xLocksPidToTid.get(pid);
            if (xTid != null) {
              if (!tid.equals(xTid)) {
                xLockIsFree = false;
              }
            }
            if (xLockIsFree) {
              HashSet<PageId> sPids = this.sLocksTidToPid.get(tid);
              if (sPids == null) {
                sPids = new HashSet<PageId>();
              }
              sPids.add(pid);
              this.sLocksTidToPid.put(tid, sPids);
              HashSet<TransactionId> sTids = this.sLocksPidToTid.get(pid);
              if (sTids == null) {
                sTids = new HashSet<TransactionId>();
              }
              sTids.add(tid);
              this.sLocksPidToTid.put(pid, sTids);
              break;
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
    	HashSet<TransactionId> sTids = this.sLocksPidToTid.get(pid);
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

    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	TransactionId xTid = this.xLocksPidToTid.get(pid);
    	HashSet<TransactionId> sTids = this.sLocksPidToTid.get(pid);
        HashSet<PageId> sPids = this.sLocksTidToPid.get(tid);
        HashSet<PageId> xPids = this.xLocksTidToPid.get(tid);
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
    
    public HashSet<PageId> getXLockStatus(TransactionId tid) {
        return this.xLocksTidToPid.get(tid);
    }
    
    public HashSet<PageId> getSLockStatus(TransactionId tid) {
        return this.sLocksTidToPid.get(tid);
    }
}
