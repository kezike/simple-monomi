package simpledb;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of encrypted IntFields.
 */
public class EncryptedBigIntegerAggregator implements EncryptedAggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final EncOp op;
    private final ArrayList<Tuple> results = new ArrayList<Tuple>();
    private final ConcurrentHashMap<Field, BigInteger> gbValues = 
            new ConcurrentHashMap<Field, BigInteger>();
    private final ConcurrentHashMap<Field, BigInteger> avgCount = 
            new ConcurrentHashMap<Field, BigInteger>();
    private TupleDesc td;
    // Encrypted columns show have names corresponding to these
    private int modIndex;
    private int gIndex;
    private int N;
    private int N2;
    private int G;
    Paillier_PublicKey publicKey;
    
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

    public EncryptedBigIntegerAggregator(int gbfield, Type gbfieldtype, int afield, EncOp what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
    }
    
    private void initializePublicKey(Tuple t) {
        TupleDesc td = t.getTupleDesc();
        this.modIndex = td.fieldNameToIndex(HeapFile.PAILLIER_MODULUS);
        this.gIndex = td.fieldNameToIndex(HeapFile.PAILLIER_G);
        
        this.N = ((IntField) t.getField(modIndex)).getValue();
        this.N2 = N*N;
        this.G = ((IntField) t.getField(gIndex)).getValue();
        this.publicKey = 
                new Paillier_PublicKey(BigInteger.valueOf(N), BigInteger.valueOf(N2), 
                              BigInteger.valueOf(G), HeapFile.BITS_INTEGER);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the encrypted Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // This field could be either a StringField or an IntField
        Field gbField = gbfield != Aggregator.NO_GROUPING ? 
                tup.getField(gbfield) : // IntField
                new IntField(gbfield);  // -1
                
        BigInteger aggVal = BigInteger.valueOf(((IntField) tup.getField(afield)).getValue());
        
        if (!gbValues.containsKey(gbField)) {
            gbValues.put(gbField, op.equals(EncOp.COUNT) ? BigInteger.valueOf(1) : aggVal);
            avgCount.putIfAbsent(gbField, BigInteger.valueOf(1));
        } else {            
            BigInteger prevVal = gbValues.get(gbField);
            
            switch (this.op) {    
            case PAILLIER_SUM:
                // TODO: Replace with actual implementation of Paillier sum
                td = tup.getTupleDesc();
                if (publicKey == null) {
                    initializePublicKey(tup);
                }
                // Encrypted columns show have names corresponding to these
                gbValues.put(gbField, Paillier.add(prevVal, aggVal, publicKey));
                break;
            case PAILLIER_AVG:
                // TODO: Replace with Paillier implementation
                td = tup.getTupleDesc();
                if (publicKey == null) {
                    initializePublicKey(tup);
                }
                gbValues.put(gbField, Paillier.add(prevVal, aggVal, publicKey));
                avgCount.put(gbField, avgCount.get(gbField).add(BigInteger.ONE));
                
                //Integer num = prevVal + aggVal;
                //gbValues.put(gbField, num);
                break;
            case OPE_MIN:
                // TODO: Replace with OPE implementation
                gbValues.put(gbField, OPE.min(prevVal, aggVal));
                break;
            case OPE_MAX:
                // TODO: Replace with OPE implementation
                gbValues.put(gbField, OPE.max(prevVal, aggVal));
                break;
            case COUNT:
                gbValues.put(gbField, prevVal.add(BigInteger.ONE));
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
            BigInteger value = (BigInteger) gbValues.get(new IntField(gbfield));
            
            // Multiply by one over count to divide TODO: Test that this works
            if (op.equals(EncOp.PAILLIER_AVG)) {
                value = Paillier.constMult(value, BigInteger.ONE.divide(avgCount.get(gb)), publicKey);
            }
            t.setField(0, new IntField(value.intValueExact()));
            results.add(t);
            return new TupleIterator(td, results);
        } else {
            // Regular case, return (groupVal, aggVal) pairs
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
               
            for (Field gb : gbValues.keySet()) {
                Tuple t = new Tuple(td);
                BigInteger value = gbValues.get(gb);

                // TODO: Make sure this still works with Paillier
                if (op.equals(EncOp.PAILLIER_AVG)) {
                    value = Paillier.constMult(value, BigInteger.ONE.divide(avgCount.get(gb)), publicKey);
                }
                t.setField(0, gb);
                t.setField(1, new IntField(value.intValueExact()));                    
                results.add(t);
            }
            return new TupleIterator(td, results);
        }
        
    }

}
