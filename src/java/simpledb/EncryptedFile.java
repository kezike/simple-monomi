package simpledb;

import java.io.*;
import java.util.*;

/**
 * EncryptedFile is an encrypted version of a HeapFile using Paillier encryption 
 * and OPE to encrypt its columns. The last column of the table is the public key for the
 * Paillier encryption (the modulus n)
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Akengin, Mehmet Efe; Ezike, Kayode; Henriquez, Carlos. 
 */
public class EncryptedFile implements DbFile {

    private File file;
    private TupleDesc tupDesc;

    /**
     * FileTupleIterator implements DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        
    	private EncryptedFile heapFile;
        private Iterator<Tuple> heapPageIter;
        private TransactionId txnId;
        private boolean isOpen;
        private int pgIdx;
        
        public HeapFileIterator(EncryptedFile hf, TransactionId tid) {
            this.heapFile = hf;
            this.heapPageIter = null;
            this.txnId = tid;
            this.isOpen = false;
            this.pgIdx = -1;
        }
        
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open()
            throws DbException, TransactionAbortedException {
            this.isOpen = true;
        }
        
        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext()
            throws DbException, TransactionAbortedException {
            if (!this.isOpen) {
              return false;
            }
            if (heapPageIter != null) {
              if (heapPageIter.hasNext()) {
                return true;
              }
            }
            do {
              this.pgIdx++;
              if (this.pgIdx == this.heapFile.numPages()) {
                  return false;
              }
              BufferPool bufferPool = Database.getBufferPool();
              PageId pid = new HeapPageId(this.heapFile.getId(), this.pgIdx);
              HeapPage page = (HeapPage) bufferPool.getPage(this.txnId, pid, Permissions.READ_ONLY);
              this.heapPageIter = page.iterator();
            } while (!this.heapPageIter.hasNext());
            return true;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.hasNext()) {
              throw new NoSuchElementException("No more tuples in this file");
            }
            return this.heapPageIter.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
        	this.pgIdx = -1;
        	this.heapPageIter = null;
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.isOpen = false;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public EncryptedFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile pageRaf;
        try {
          pageRaf = new RandomAccessFile(this.file, "r");
        } catch (FileNotFoundException fnfExn) {
          throw new IllegalArgumentException("File not found");
        }
        int pageNum = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNum * pageSize;
        byte pageData[] = new byte[pageSize];
        if (pageNum < 0 || pageNum > this.numPages()) {
          try {
            pageRaf.close();
          } catch (IOException ioExn) {
          }
          throw new IllegalArgumentException("This page does not exist");
        }
        try {
          pageRaf.seek(offset);
          pageRaf.read(pageData, 0, pageSize);
          pageRaf.close();
          return new HeapPage((HeapPageId)pid, pageData);
        } catch (IOException ioExn) {
          throw new IllegalArgumentException("Page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pageNum = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNum * pageSize;
        byte[] pageData = page.getPageData();
        RandomAccessFile pageRaf = new RandomAccessFile(this.file, "rw");
        pageRaf.seek(offset);
        pageRaf.write(pageData);
        pageRaf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        return (int) Math.ceil(this.file.length() / (double) pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage page;
        HeapPageId pid;
        boolean foundPage = false;
        int numPages = this.numPages();
        for (int i = 0; i < numPages; i++) {
          pid = new HeapPageId(this.getId(), i);
          try {
            page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            pagesAffected.add(page);
            foundPage = true;
            break;
          } catch (DbException dbExn) {
            bufferPool.releasePage(tid, pid);
          }
        }
        if (!foundPage) {
          pid = new HeapPageId(this.getId(), numPages);
          int pageSize = BufferPool.getPageSize();
          int offset = numPages * pageSize;
          byte[] pageData = new byte[pageSize];
          Arrays.fill(pageData, (byte) 0);
          RandomAccessFile pageRaf = new RandomAccessFile(this.file, "rw");
          pageRaf.seek(offset);
          pageRaf.write(pageData);
          pageRaf.close();
          page = (HeapPage) this.readPage(pid);
          page.insertTuple(t);
          page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
          page.insertTuple(t);
          pagesAffected.add(page);
        }
        return pagesAffected;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        BufferPool bufferPool = Database.getBufferPool();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        pagesAffected.add(page);
        return pagesAffected;
    }

    /**
     * @return the tuples in this file
     */
    public List<Tuple> getValidTuples(TransactionId tid) {
        // Iterator should return the encrypted tuples that can be decrypted with the right keys
        ArrayList<Tuple> newTuples = new ArrayList<Tuple>();
        for (int i = 0; i < this.numPages(); i++) {
          PageId pid = new HeapPageId(this.getId(), i);
          BufferPool bufferPool = Database.getBufferPool();
          HeapPage page;
          try {
            page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
          } catch (TransactionAbortedException txnAbExn) {
            throw new NoSuchElementException("Invalid page access");
          } catch (DbException dbExn) {
            throw new NoSuchElementException("Invalid page access");
          }
          List<Tuple> tuples = page.getValidTuples();
          newTuples.addAll(tuples);
        }
        return newTuples;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

