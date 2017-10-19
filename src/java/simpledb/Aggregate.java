package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aField;
    private int gbField;
    private Aggregator.Op aOprtr;
    private Aggregator aggr;
    private OpIterator aggrIter;
    private TupleDesc tupDesc;
    private OpIterator[] children;
    private boolean isOpen;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gbfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gbfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.aField = afield;
        this.gbField = gbfield;
        this.aOprtr = aop;
        this.isOpen = false;
        this.tupDesc = this.child.getTupleDesc();
        Type gbFieldType;
        if (this.hasGrouping()) {
          gbFieldType = this.tupDesc.getFieldType(gbfield);
        } else {
          gbFieldType = null;
        }
        Type aFieldType = this.tupDesc.getFieldType(afield);
        if (aFieldType == Type.INT_TYPE) {
          this.aggr = new IntegerAggregator(gbfield, gbFieldType, afield, aop);
        } else {
          this.aggr = new StringAggregator(gbfield, gbFieldType, afield, aop);
        }
        this.mergeAllTuples(child, this.aggr);
        this.aggrIter = this.aggr.iterator();
    }

    /**
     * @return whether or not aggregator has grouping
     */
    public boolean hasGrouping() {
        return this.gbField != Aggregator.NO_GROUPING;
    }

    /**
     * Merge all tuples of a child OpIterator into their appropriate group
     * @param child the OpIterator feeding tuples
     */
    public void mergeAllTuples(OpIterator child, Aggregator aggr) {
        try {
          child.open();
          while (child.hasNext()) {
            Tuple next = child.next();
            aggr.mergeTupleIntoGroup(next);
          }
          child.rewind();
          child.close();
        } catch (DbException dbExn) {
        } catch (TransactionAbortedException txnAbExn) {
        }
    }
    
    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // some code goes here
        return this.gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (this.hasGrouping()) {
          return this.tupDesc.getFieldName(this.gbField);
        }
	return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
        return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    // some code goes here
        return this.tupDesc.getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
        return this.aOprtr;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
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

    public void open() throws NoSuchElementException, DbException,
        TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.aggrIter.open();
        this.isOpen = true;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        Tuple next;
        try {
          next = this.aggrIter.next();
        } catch (NoSuchElementException nseExn) {
          next = null;
        }
        return next;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.checkOpen();
        this.child.rewind();
        this.aggrIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
	    return this.tupDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.isOpen = false;
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
