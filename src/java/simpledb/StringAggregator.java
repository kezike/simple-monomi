package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static Field NO_GROUPING_FIELD = new IntField(NO_GROUPING);
    private ConcurrentHashMap<Field, Tuple> aggrByGroup;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op oprtr;

    private class StringAggregatorIterator implements OpIterator {
        private ConcurrentHashMap<Field, Tuple> aggrByGroup;
        private Enumeration<Field> strAggrIter;
        private TupleDesc tupDesc;
        private boolean isOpen;

        public StringAggregatorIterator(ConcurrentHashMap<Field, Tuple> aggrByGroup) {
            this.aggrByGroup = aggrByGroup;
            this.strAggrIter = aggrByGroup.keys();
            this.tupDesc = null;
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
            if (!this.isOpen) {
              return false;
            }
            return this.strAggrIter.hasMoreElements();
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
            Field nextField = this.strAggrIter.nextElement();
            Tuple nextTuple = this.aggrByGroup.get(nextField);
            this.tupDesc = nextTuple.getTupleDesc();
            return nextTuple;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            this.strAggrIter = aggrByGroup.keys();
        }

        /**
         * Returns the TupleDesc associated with this OpIterator.
         * @return the TupleDesc associated with this OpIterator.
         */
        public TupleDesc getTupleDesc() {
            return this.tupDesc;
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.isOpen = false;
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
          throw new IllegalArgumentException("String aggregator only suports COUNT operator");
        }
        this.aggrByGroup = new ConcurrentHashMap<Field, Tuple>();
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.oprtr = what;
    }

    /**
     * @return whether or not aggregator has grouping
     */
    public boolean hasGrouping() {
        return this.gbField != NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        TupleDesc tupDesc = tup.getTupleDesc();
        Field tupGBField = tup.getField(this.gbField);
        StringField tupAField = (StringField) tup.getField(this.aField);
        Tuple tupAggr;
        String tupAggrGBFieldName;
        String tupAggrAFieldName;
        Type[] tupAggrFieldTypes;
        String[] tupAggrFieldNames;
        TupleDesc tupAggrDesc;
        Field tupAggrGBField;
        IntField tupAggrAField;
        int gbIdx = 0;
        int aIdx;
        if (this.hasGrouping()) {
          tupAggrGBFieldName = tupDesc.getFieldName(this.gbField);
          tupAggrAFieldName = tupDesc.getFieldName(this.aField);
          tupAggrFieldTypes = new Type[]{this.gbFieldType, Type.INT_TYPE};
          tupAggrFieldNames = new String[]{tupAggrGBFieldName, tupAggrAFieldName};
          tupAggrDesc = new TupleDesc(tupAggrFieldTypes, tupAggrFieldNames);
          tupAggrGBField = tup.getField(this.gbField);
          aIdx = 1;
        } else {
          tupAggrAFieldName = tupDesc.getFieldName(this.aField);
          tupAggrFieldTypes = new Type[]{Type.INT_TYPE};
          tupAggrFieldNames = new String[]{tupAggrAFieldName};
          tupAggrDesc = new TupleDesc(tupAggrFieldTypes, tupAggrFieldNames);
          tupAggrGBField = NO_GROUPING_FIELD;
          aIdx = 0;
        }
        tupAggr = this.aggrByGroup.get(tupAggrGBField);
        if (tupAggr == null) {
          tupAggr = new Tuple(tupAggrDesc);
          if (this.hasGrouping()) {
            tupAggr.setField(gbIdx, tupAggrGBField);
          }
          tupAggr.setField(aIdx, new IntField(0));
        }
        tupAggrAField = (IntField) tupAggr.getField(aIdx);
        int tupAggrAFieldVal = tupAggrAField.getValue();
        tupAggr.setField(aIdx, new IntField(tupAggrAFieldVal + 1));
        this.aggrByGroup.put(tupAggrGBField, tupAggr);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator(new ConcurrentHashMap<Field, Tuple>(this.aggrByGroup));
    }

}
