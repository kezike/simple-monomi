package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tblId;
    private TupleDesc tupDesc;
    private OpIterator[] children;
    private boolean inserted;
    private boolean isOpen;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
        throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tblId = tableId;
        Type[] tdFieldTypes = new Type[]{Type.INT_TYPE};
        String[] tdFieldNames = new String[]{"numInserted"};
        this.tupDesc = new TupleDesc(tdFieldTypes, tdFieldNames);
        this.inserted = false;
        this.isOpen = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupDesc;
    }

    /**
     * Checks if iterator is open
     * @return open status
     */
    private void checkOpen() {
        if (!this.isOpen) {
          throw new IllegalStateException("Iterator is not open");
        }
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.isOpen = true;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.checkOpen();
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.inserted) {
          return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int numInserted = 0;
        while (this.child.hasNext()) {
          try {
            Tuple childNext = this.child.next();
            bufferPool.insertTuple(this.tid, this.tblId, childNext);
            numInserted++;
          } catch (IOException ioExn) {
          }
        }
        this.inserted = true;
        Tuple next = new Tuple(this.tupDesc);
        next.setField(0, new IntField(numInserted));
        return next;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }
}
