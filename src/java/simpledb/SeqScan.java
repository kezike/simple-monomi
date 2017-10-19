package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId txnId;
    private int tblId;
    private String tblAlias;
    private DbFile file;
    private DbFileIterator fileIter;
    private boolean isOpen;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.txnId = tid;
        this.tblId = tableid;
        if (tableAlias == null) {
          this.tblAlias = "null";
        } else {
          this.tblAlias = tableAlias;
        }
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.fileIter = this.file.iterator(tid);
        this.isOpen = false;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     **/
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tblId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     **/
    public String getAlias()
    {
        // some code goes here
        return this.tblAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tblId = tableid;
        this.tblAlias = tableAlias;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.fileIter = this.file.iterator(this.txnId);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
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
        this.isOpen = true;
        this.fileIter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // this.checkOpen();
        TupleDesc tupDesc = Database.getCatalog().getTupleDesc(this.tblId);
        ArrayList<TDItem> items = new ArrayList<TDItem>();
        for (TDItem tdItem : tupDesc.getItems()) {
          String fieldName = tdItem.getFieldName();
          fieldName = this.tblAlias + "." + fieldName;
          Type fieldType = tdItem.getFieldType();
          items.add(new TDItem(fieldType, fieldName));
        }
        return new TupleDesc(items);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        this.checkOpen();
        return this.fileIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        this.checkOpen();
        return this.fileIter.next();
    }

    public void close() {
        // some code goes here
        this.fileIter.close();
        this.isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.checkOpen();
        this.fileIter.rewind();
    }
}
