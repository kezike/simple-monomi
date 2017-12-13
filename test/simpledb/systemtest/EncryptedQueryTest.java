package simpledb.systemtest;

import simpledb.*;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import junit.framework.JUnit4TestAdapter;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

public class EncryptedQueryTest {

    private HeapFile table;
    private DbFileIterator tableIter;
    private EncryptedFile tableEnc;

    @Before
    public void setupEncryptedQueryTest() throws IOException, DbException, TransactionAbortedException {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "a", "b", "c" };
        TupleDesc td = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        this.table = new HeapFile(new File("test/simpledb/end_to_end_enc_test.dat"), td);
        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "end_to_end_enc_test");
        this.tableIter = this.table.iterator(new TransactionId());
        this.tableEnc = this.table.encrypt();
        catalog.addTable(this.tableEnc, "end_to_end_enc_test_enc");
    }

    @Test public void testEndToEndMax() {
        /*
        // TODO: Read query from parser
        Transaction t = new Transaction();
        t.start();
        Parser p = new Parser();
        p.setTransaction(t);
        p.processNextStatement("SELECT ope_max(a),* FROM end_to_end_enc_test;");*/
        // construct the query
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, this.tableEnc.getId());
        int fieldIdx = this.tableEnc.getTupleDesc().fieldNameToIndex("OPE_b");
        EncryptedAggregate max = new EncryptedAggregate(seqScan, fieldIdx, Aggregator.NO_GROUPING, EncryptedAggregator.EncOp.OPE_MAX);
        try {
          max.open();
          // while (max.hasNext()) {
          Tuple tuple = max.next();
          System.out.println("OPE_MAX(b): " + tuple);
          // }
          max.close();
          Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
          System.out.println ("Exception: " + e);
        }
    }
}
