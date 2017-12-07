package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private TupleDesc tupDesc;
    private OpIterator[] children;
    private boolean deleted;
    private boolean isOpen;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        Type[] tdFieldTypes = new Type[]{Type.INT_TYPE};
        String[] tdFieldNames = new String[]{"numDeleted"};
        this.tupDesc = new TupleDesc(tdFieldTypes, tdFieldNames);
        this.deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.deleted) {
          return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int numDeleted = 0;
        while (this.child.hasNext()) {
          try {
            Tuple childNext = this.child.next();
            bufferPool.deleteTuple(this.tid, childNext);
            numDeleted++;
          } catch (IOException ioExn) {
          }
        }
        this.deleted = true;
        Tuple next = new Tuple(this.tupDesc);
        next.setField(0, new IntField(numDeleted));
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
