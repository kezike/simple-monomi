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

    private File file;
    private TupleDesc tupDesc;

    /**
     * FileTupleIterator implements DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        
        private int position;
        private boolean isOpen;
        private List<?> tuples;
        
        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.tuples = hf.getValidTuples(tid);
            this.isOpen = false;
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
            return (this.position < this.tuples.size() && this.isOpen);
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
            Object tupObj = this.tuples.get(this.position);
            this.position++;
            return (Tuple) tupObj;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            this.position = 0;
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.isOpen = false;
        }
        
        /**
         * Check if iterator is Open
         * @return open status of iterator
         */
        public boolean isOpen() {
            return this.isOpen;
        }

    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
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
        int pgNum = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();
        int offset = pgNum * pgSize;
        byte pgData[] = new byte[pgSize];
        if (pgNum < 0 || pgNum > this.numPages()) {
          throw new IllegalArgumentException("This page does not exist");
        }
        try {
          pageRaf.seek(offset);
          pageRaf.read(pgData, 0, pgSize);
          pageRaf.close();
          return new HeapPage((HeapPageId)pid, pgData);
        } catch (IOException ioExn) {
          throw new IllegalArgumentException("Page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pgSize = BufferPool.getPageSize();
        return (int) Math.ceil(this.file.length() / (double) pgSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    /**
     * @return the tuples in this file
     */
    public List<Tuple> getValidTuples(TransactionId tid) {
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

