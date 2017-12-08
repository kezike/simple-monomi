package simpledb;

import java.io.Serializable;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples. An EncryptedAggregagor requires knowledge of the EncryptionScheme 
 * that the Tuples were encrypted with in order to determine what operations are allowed
 * given an EncryptionScheme.
 * 
 * @see simpledb.EncryptedFile
 */
public interface EncryptedAggregator extends Serializable, Aggregator {
    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum EncOp implements Serializable {
        OPE_MIN, OPE_MAX, PAILLIER_SUM, PAILLIER_AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT, // probably not needed for project
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG; // probably not needed for project

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static EncOp getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static EncOp getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
            if (this==OPE_MIN)
                return "OPE_min";
            if (this==OPE_MAX)
                return "OPE_max";
            if (this==PAILLIER_SUM)
                return "PAILLIER_sum";
            if (this==SUM_COUNT)
                return "sum_count";
            if (this==PAILLIER_AVG)
                return "PAILLIER_avg";
            if (this==COUNT)
                return "count";
            if (this==SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public OpIterator iterator();
    
}
