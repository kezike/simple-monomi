package simpledb;

import java.util.concurrent.ConcurrentHashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int numBuckets;
    private int min;
    private int max;
    private int numTuples;
    private ConcurrentHashMap<Integer, Integer> countByBucket;
    private ConcurrentHashMap<Integer, Integer> minByBucket;
    private ConcurrentHashMap<Integer, Integer> maxByBucket;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numBuckets = Math.min(buckets, max - min);
        this.min = min;
        this.max = max;
        this.numTuples = 0;
        this.countByBucket = new ConcurrentHashMap<Integer, Integer>();
        this.minByBucket = new ConcurrentHashMap<Integer, Integer>();
        this.maxByBucket = new ConcurrentHashMap<Integer, Integer>();
        for (int i = 0; i < this.numBuckets; i++) {
          this.countByBucket.put(i, 0);
        }
    }

    /**
     * Compute the bucket for a given value
     * @param v Value of interest
     */
    private int computeBucket(int v) {
        if (v < this.min) {
          return Integer.MIN_VALUE;
        }
        if (v > this.max) {
          return Integer.MAX_VALUE;
        }
        double inc = (this.max - this.min) / (new Integer(this.numBuckets)).doubleValue();
        double cumVal = this.min + inc;
        int bucket = 0;
        while (cumVal < v) {
          cumVal += inc;
          bucket++;
        }
        return bucket;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucket = this.computeBucket(v);
        bucket = Math.max(0, Math.min(this.numBuckets - 1, bucket)); 
        Integer bucketCount = this.countByBucket.get(bucket);
        Integer bucketMin = this.minByBucket.get(bucket);
        Integer bucketMax = this.maxByBucket.get(bucket);
        if (bucketMin == null) {
          this.minByBucket.put(bucket, v);
        } else if (v < bucketMin) {
          this.minByBucket.put(bucket, v);
        }
        if (bucketMax == null) {
          this.maxByBucket.put(bucket, v);
        } else if (v > bucketMax) {
          this.maxByBucket.put(bucket, v);
        }
        /*if (bucketCount == null) {
          bucketCount = 0;
        }*/
        this.countByBucket.put(bucket, bucketCount + 1);
        this.numTuples++;
    }

    /**
     * Estimate the selectivity of "EQUALS" and operand on this table.
     * 
     * @param v operand value
     * @return Predicted selectivity of "EQUALS" and v
     */
    private double estimateSelectivityEq(int v) {
        int bucket = this.computeBucket(v);
        if (bucket == Integer.MIN_VALUE || bucket == Integer.MAX_VALUE) {
          return 0.0;
        }
        Integer bucketCount = this.countByBucket.get(bucket);
        if (bucketCount == 0) {
          return 0.0;
        }
        Integer bucketMin = this.minByBucket.get(bucket);
        Integer bucketMax = this.maxByBucket.get(bucket);
        Integer bucketRange = bucketMax - bucketMin;
        if (bucketRange == 0) {
          if (bucketMin == v) {
            return bucketCount.doubleValue() / this.numTuples;
          }
          return 0.0;
        }
        return (bucketCount.doubleValue() / bucketRange) / numTuples;
    }

    /**
     * Estimate the selectivity of "GREATER_THAN" and operand on this table.
     * 
     * @param v operand value
     * @return Predicted selectivity of "GREATER_THAN" and v
     */
    private double estimateSelectivityGt(int v) {
        int bucket = this.computeBucket(v);
        if (bucket == Integer.MIN_VALUE) {
          return 1.0;
        } else if (bucket == Integer.MAX_VALUE) {
          return 0.0;
        }
        Integer bucketCount = this.countByBucket.get(bucket);
        Integer bucketMin = this.minByBucket.get(bucket);
        Integer bucketMax = this.maxByBucket.get(bucket);
        Integer bucketRange;
        if (bucketCount == 0) {
          bucketRange = 0;
        } else {
          bucketRange = bucketMax - bucketMin;
        }
        double gtInnerSel;
        double gtOuterSel;
        double gtSel = 0;
        if (bucketRange != 0) {
          gtInnerSel = (bucketMax.doubleValue() - v) / bucketRange;
          gtOuterSel = bucketCount.doubleValue() / this.numTuples;
          gtSel = gtInnerSel * gtOuterSel;
        }
        for (int i = bucket + 1; i < this.numBuckets; i++) {
          bucketCount = this.countByBucket.get(i);
          gtInnerSel = 1.0;
          gtOuterSel = bucketCount.doubleValue() / this.numTuples;
          gtSel += gtInnerSel * gtOuterSel;
        }
        return gtSel;
    }

    /**
     * Estimate the selectivity of "LESS_THAN" and operand on this table.
     * 
     * @param v operand value
     * @return Predicted selectivity of "LESS_THAN" and v
     */
    private double estimateSelectivityLt(int v) {
        int bucket = this.computeBucket(v);
        if (bucket == Integer.MIN_VALUE) {
          return 0.0;
        } else if (bucket == Integer.MAX_VALUE) {
          return 1.0;
        }
        Integer bucketCount = this.countByBucket.get(bucket);
        Integer bucketMin = this.minByBucket.get(bucket);
        Integer bucketMax = this.maxByBucket.get(bucket);
        Integer bucketRange = 0;
        if (bucketCount != 0) {
          bucketRange = bucketMax - bucketMin;
        }
        double gtInnerSel;
        double gtOuterSel;
        double gtSel = 0;
        if (bucketRange != 0) {
          gtInnerSel = (v - bucketMin.doubleValue()) / bucketRange;
          gtOuterSel = bucketCount.doubleValue() / this.numTuples;
          gtSel = gtInnerSel * gtOuterSel;
        }
        for (int i = bucket - 1; i >= 0; i--) {
          bucketCount = this.countByBucket.get(i);
          gtInnerSel = 1.0;
          gtOuterSel = bucketCount.doubleValue() / this.numTuples;
          gtSel += gtInnerSel * gtOuterSel;
        }
        return gtSel;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        if (this.numTuples == 0) {
          return 0.0;
        }
        switch (op) {
          case EQUALS:
            return estimateSelectivityEq(v);
          case GREATER_THAN:
            return estimateSelectivityGt(v);
          case LESS_THAN:
            return estimateSelectivityLt(v);
          case LESS_THAN_OR_EQ:
            return 1.0 - estimateSelectivityGt(v);
          case GREATER_THAN_OR_EQ:
            return 1.0 - estimateSelectivityLt(v);
          case LIKE:
            return estimateSelectivityEq(v);
          case NOT_EQUALS:
            return 1.0 - estimateSelectivityEq(v);
          default:
            return 0.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     **/
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return the number of tuples of the underlying table
     **/
    public int numTuples() {
        return this.numTuples;
    }

    /** @return the minimum value indexed by the histogram */
    public int minVal() {
        return this.min;
    }

    /** @return the maximum value indexed by the histogram */
    public int maxVal() {
        return this.max;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     **/
    public String toString() {
        // some code goes here
        StringBuilder ihStrBldr = new StringBuilder();
        // TODO
        ihStrBldr.append("INT HISTOGRAM");
        return ihStrBldr.toString();
    }
}
