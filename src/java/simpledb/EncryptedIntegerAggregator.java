package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of encrypted IntFields.
 */
public class EncryptedIntegerAggregator implements EncryptedAggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final EncOp op;
    private final ArrayList<Tuple> results = new ArrayList<Tuple>();
    private final ConcurrentHashMap<Field, Integer> gbValues = new ConcurrentHashMap<Field, Integer>();
    private final ConcurrentHashMap<Field, Integer> avgCount = new ConcurrentHashMap<Field, Integer>();
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     *            
     * TODO: The result returned from this will be an encrypted result, need to 
     * then hand this to the client to decrypt with the appropriate KeyPair
     */

    public EncryptedIntegerAggregator(int gbfield, Type gbfieldtype, int afield, EncOp what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // This field could be either a StringField or an IntField
        Field gbField = gbfield != Aggregator.NO_GROUPING ? 
                tup.getField(gbfield) : // IntField
                new IntField(gbfield);  // -1
                
        Integer aggVal = Integer.valueOf(((IntField) tup.getField(afield)).getValue());
        
        if (!gbValues.containsKey(gbField)) {
            gbValues.put(gbField, op.equals(Op.COUNT) ? 1 : aggVal);
            avgCount.putIfAbsent(gbField, Integer.valueOf(1));
        } else {            
            Integer prevVal = gbValues.get(gbField);
            
            switch (this.op) {    
            case PAILLIER_SUM:
                // TODO: Replace with actual implementation of Paillier sum
                gbValues.put(gbField, (prevVal + aggVal));
                break;
            case PAILLIER_AVG:
                // TODO: Replace with Paillier implementation
                Integer num = prevVal + aggVal;
                gbValues.put(gbField, num);
                avgCount.put(gbField, avgCount.get(gbField) +1);
                break;
            case OPE_MIN:
                // TODO: Replace with OPE implementation
                gbValues.put(gbField, prevVal < aggVal ? prevVal : aggVal);
                break;
            case OPE_MAX:
                // TODO: Replace with OPE implementation
                gbValues.put(gbField, prevVal < aggVal ? aggVal : prevVal);
                break;
            case COUNT:
                gbValues.put(gbField, prevVal + 1);
            case SUM_COUNT:
            case SC_AVG:
                //throw new NoSuchElementException("This will be implemented in lab7");
            }
        }
        
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        if (gbfield == Aggregator.NO_GROUPING) {
            // No group case, return single value
            TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            Field gb = new IntField(gbfield);
            Integer value = gbValues.get(new IntField(gbfield));

            // TODO: Make sure this still works with Paillier
            if (op.equals(Op.AVG)) value /= avgCount.get(gb); // late average
            t.setField(0, new IntField(value));
            results.add(t);
            return new TupleIterator(td, results);
        } else {
            // Regular case, return (groupVal, aggVal) pairs
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for (Field gb : gbValues.keySet()) {
                Tuple t = new Tuple(td);
                Integer value = gbValues.get(gb);

                // TODO: Make sure this still works with Paillier
                if (op.equals(Op.AVG)) value /= avgCount.get(gb); // late average
                t.setField(0, gb);
                t.setField(1, new IntField(value));                    
                results.add(t);
            }
            return new TupleIterator(td, results);
        }
        
    }

}
