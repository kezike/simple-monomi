package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.NoSuchElementException;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tblId;
    private int ioPageCost;
    private int numTuples;
    private ArrayList<Object> histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tblId = tableid;
        this.ioPageCost = ioCostPerPage;
        this.histograms = new ArrayList<Object>();
        Catalog catalog = Database.getCatalog();
        DbFile table = catalog.getDatabaseFile(tableid);
        TupleDesc tupDesc = table.getTupleDesc();
        DbFileIterator tblIter = table.iterator(new TransactionId());
        try {
          tblIter.open();
          for (int i = 0; i < tupDesc.numFields(); i++) {
            Type fieldType = tupDesc.getFieldType(i);
            if (fieldType == Type.INT_TYPE) {
              int min = Integer.MAX_VALUE;
              int max = Integer.MIN_VALUE;
              while (tblIter.hasNext()) {
                Tuple next = tblIter.next();
                IntField nextField = (IntField) (next.getField(i));
                int nextVal = nextField.getValue();
                if (nextVal < min) {
                  min = nextVal;
                }
                if (nextVal > max) {
                  max = nextVal;
                }
              }
              Object histogram = (Object) (new IntHistogram(NUM_HIST_BINS, min, max));
              this.histograms.add(histogram);
            } else {
              Object histogram = (Object) (new StringHistogram(NUM_HIST_BINS));
              this.histograms.add(histogram);
            }
            tblIter.rewind();
          }
          for (int i = 0; i < tupDesc.numFields(); i++) {
            Type fieldType = tupDesc.getFieldType(i);
            if (fieldType == Type.INT_TYPE) {
              IntHistogram intHist = (IntHistogram) this.histograms.get(i);
              while (tblIter.hasNext()) {
                Tuple next = tblIter.next();
                IntField nextField = (IntField) (next.getField(i));
                int nextVal = nextField.getValue();
                intHist.addValue(nextVal);
              }
              this.numTuples = intHist.numTuples();
            } else {
              StringHistogram strHist = (StringHistogram) this.histograms.get(i);
              while (tblIter.hasNext()) {
                Tuple next = tblIter.next();
                StringField nextField = (StringField) (next.getField(i));
                String nextVal = nextField.getValue();
                strHist.addValue(nextVal);
              }
              this.numTuples = strHist.numTuples();
            }
            tblIter.rewind();
          }
          tblIter.close();
        } catch (DbException dbExn) {
        } catch (TransactionAbortedException txnAbExn) {
        } catch (NoSuchElementException nseExn) {
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        Catalog catalog = Database.getCatalog();
        DbFile table = catalog.getDatabaseFile(this.tblId);
        int numPages = ((HeapFile) table).numPages();
        return numPages * this.ioPageCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // return (int) Math.round(Math.floor(selectivityFactor * this.numTuples));
        return (int) (selectivityFactor * this.numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        /*double avgSel = 0.0;
        Type fieldType = tupDesc.getFieldType(field);
        if (fieldType == Type.INT_TYPE) {
          IntHistogram intHist = (IntHistogram) this.histograms.get(field);
          double valProb = 1.0 / (intHist.maxVal() - intHist.minVal());
          for (int i = intHist.minVal(); i <= intHist.maxVal(); i++) {
            avgSel += valProb * intHist.estimateSelectivity(op, i);
          }
        } else {
          StringHistogram strHist = (StringHistogram) this.histograms.get(field);
          double valProb = 1.0 / (strHist.maxVal() - strHist.minVal());
          for (int i = strHist.minVal(); i <= strHist.maxVal(); i++) {
            avgSel += valProb * strHist.estimateSelectivity(op, );
          }
        }
        return avgSel;*/
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = constant.getType();
        if (fieldType == Type.INT_TYPE) {
          int fieldConstant = ((IntField) constant).getValue();
          IntHistogram intHist = (IntHistogram) this.histograms.get(field);
          return intHist.estimateSelectivity(op, fieldConstant);
        } else {
          String fieldConstant = ((StringField) constant).getValue();
          StringHistogram strHist = (StringHistogram) this.histograms.get(field);
          return strHist.estimateSelectivity(op, fieldConstant);
        }
    }

    /**
     * @param field field of interest 
     * @return the minimum value of field
     **/
    public int minVal(int field) {
        IntHistogram intHist = (IntHistogram) this.histograms.get(field);
        return intHist.minVal();
    }

    /**
     * @param field field of interest 
     * @return the maximum value of field
     **/
    public int maxVal(int field) {
        IntHistogram intHist = (IntHistogram) this.histograms.get(field);
        return intHist.maxVal();
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
