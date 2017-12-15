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
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class EncryptedQueryTest {

    private HeapFile table;
    private EncryptedFile tableEnc;
    private DbFileIterator tableIter;
    private DbFileIterator tableEncIter;
    private ConcurrentHashMap<String, KeyPair> keyPairs;
    private Paillier_KeyPair paillerKeyPair;
    private OPE_PrivateKey opePrivateKey;
    private OPE_PublicKey opePublicKey;
    private OPE_KeyPair opeKeyPair;

    @Before
    public void setupEncryptedQueryTest() throws IOException, DbException, TransactionAbortedException {
    	// Setup Paillier key pairs for encryption/decryption
    	Paillier_KeyPairBuilder paillierKeyGen = new Paillier_KeyPairBuilder();
        paillierKeyGen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        paillierKeyGen.bits(HeapFile.BITS_INTEGER);
        this.paillerKeyPair = paillierKeyGen.generateKeyPair();
        
        // Setup OPE key pairs for encryption/decryption
    	OPE_CipherPrivate opeCipherPrivate = new OPE_CipherPrivate.Mult(BigInteger.valueOf(5));
        OPE_CipherPublic opeCipherPublic = new OPE_CipherPublic.Mult(BigInteger.valueOf(5));
        this.opePrivateKey = new OPE_PrivateKey(opeCipherPrivate);
        this.opePublicKey = new OPE_PublicKey(opeCipherPublic);
        this.opeKeyPair = new OPE_KeyPair(this.opePrivateKey, this.opePublicKey);
        this.keyPairs = new ConcurrentHashMap<String, KeyPair>();
        
        this.keyPairs.put(HeapFile.PAILLIER_PREFIX, (KeyPair) this.paillerKeyPair);
        this.keyPairs.put(HeapFile.OPE_PREFIX, (KeyPair) this.opeKeyPair);
    	
    	// Construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "a", "b", "c" };
        TupleDesc td = new TupleDesc(types, names);

        // Create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        this.table = new HeapFile(new File("test/simpledb/end_to_end_enc_test.dat"), td);
        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "end_to_end_enc_test");
        this.tableIter = this.table.iterator(new TransactionId());
        this.tableEnc = this.table.encrypt(this.keyPairs);
        catalog.addTable(this.tableEnc, "end_to_end_enc_test_enc");
        System.out.println("Original Table");
        this.tableIter.open();
        while (this.tableIter.hasNext()) {
          Tuple tuple = this.tableIter.next();
          System.out.println(tuple);
        }
        this.tableIter.close();
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------");
        this.tableEncIter = this.tableEnc.iterator(new TransactionId());
        System.out.println("Encrypted Table");
        this.tableEncIter.open();
        while (this.tableEncIter.hasNext()) {
          Tuple tupleEnc = this.tableEncIter.next();
          System.out.println(tupleEnc);
        }
        this.tableEncIter.close();
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------");
    }
    
    @Test
    public void testEndToEndOPEQueryMax() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	System.out.println("Original Query: SELECT MAX(b) FROM end_to_end_enc_test");
        System.out.println("Encrypted Query: SELECT OPE_MAX(OPE_b) FROM end_to_end_enc_test_enc");
    	TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, this.tableEnc.getId());
        String col = "b";
        int fieldIdx = this.tableEnc.getTupleDesc().fieldNameToIndex(HeapFile.OPE_PREFIX + col);
        EncryptedAggregate max = new EncryptedAggregate(seqScan, fieldIdx, Aggregator.NO_GROUPING, EncryptedAggregator.EncOp.OPE_MAX);
        Tuple tupleMax;
        max.open();
        tupleMax = max.next();
        System.out.println(HeapFile.OPE_PREFIX + "MAX(" + col + "): " + tupleMax);
        max.close();
        Database.getBufferPool().transactionComplete(tid);
        System.out.println("MAX(" + col + "): " + this.opePrivateKey.decrypt(BigInteger.valueOf(((IntField) tupleMax.getField(0)).getValue())) + '\n');
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------");
    }
    
    @Test
    public void testEndToEndOPEQueryMin() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	// System.out.println("Original Query: SELECT MIN(c) FROM end_to_end_enc_test");
    	System.out.println("Encrypted Query: SELECT OPE_MIN(OPE_c) FROM end_to_end_enc_test_enc");
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, this.tableEnc.getId());
        String col = "c";
        int fieldIdx = this.tableEnc.getTupleDesc().fieldNameToIndex(HeapFile.OPE_PREFIX + col);
        EncryptedAggregate min = new EncryptedAggregate(seqScan, fieldIdx, Aggregator.NO_GROUPING, EncryptedAggregator.EncOp.OPE_MIN);
        Tuple tupleMin;
        min.open();
        tupleMin = min.next();
        System.out.println(HeapFile.OPE_PREFIX + "MIN(" + col + "): " + tupleMin);
        min.close();
        Database.getBufferPool().transactionComplete(tid);
        System.out.println("MIN(" + col + "): " + this.opePrivateKey.decrypt(BigInteger.valueOf(((IntField) tupleMin.getField(0)).getValue())) + '\n');
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------");
    }
        
    @Test
    public void testEndToEndOPEQueryFilter() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	// System.out.println("Original Query: SELECT a FROM end_to_end_enc_test WHERE a > 30");
    	System.out.println("Encrypted Query: SELECT OPE_a FROM end_to_end_enc_test_enc WHERE OPE_a > 150");
        TransactionId tid = new TransactionId();
        String col = "a";
        int fieldIdx = this.tableEnc.getTupleDesc().fieldNameToIndex(HeapFile.OPE_PREFIX + col);
        int boundEnc = this.opePublicKey.encrypt(BigInteger.valueOf(30)).intValue();
        IntField intField = new IntField(boundEnc);
        SeqScan seqScan = new SeqScan(tid, this.tableEnc.getId());
        Predicate pred = new Predicate(fieldIdx, Predicate.Op.GREATER_THAN, intField);
        Filter filter = new Filter(pred, seqScan);
        Tuple tuple;
        filter.open();
        System.out.println("Encrypted Results");
        HashSet<BigInteger> results = new HashSet<BigInteger>();
        while (filter.hasNext()) {
          tuple = filter.next();
          int resultVal = ((IntField) tuple.getField(fieldIdx)).getValue();
          BigInteger result = BigInteger.valueOf(resultVal);
          if (!results.contains(result)) {
            System.out.println(result);
          }
          results.add(result);
        }
        filter.close();
        Database.getBufferPool().transactionComplete(tid);
        System.out.println("Decrypted Results");
        for (BigInteger result : results) {
          System.out.println(this.opePrivateKey.decrypt(result).intValue());
        }
    }
    
    @Test
    public void testEndToEndQueryMax() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	System.out.println("Original Query: SELECT MAX(b) FROM end_to_end_enc_test");
        // System.out.println("Encrypted Query: SELECT OPE_MAX(OPE_b) FROM end_to_end_enc_test_enc");
    	TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, this.table.getId());
        String col = "b";
        int fieldIdx = this.table.getTupleDesc().fieldNameToIndex(col);
        Aggregate max = new Aggregate(seqScan, fieldIdx, Aggregator.NO_GROUPING, Aggregator.Op.MAX);
        Tuple tupleMax;
        max.open();
        tupleMax = max.next();
        System.out.println("MAX(" + col + "): " + tupleMax);
        max.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    @Test
    public void testEndToEndQueryMin() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	System.out.println("Original Query: SELECT MIN(c) FROM end_to_end_enc_test");
    	// System.out.println("Encrypted Query: SELECT OPE_MIN(OPE_c) FROM end_to_end_enc_test_enc");
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, this.table.getId());
        String col = "c";
        int fieldIdx = this.table.getTupleDesc().fieldNameToIndex(col);
        Aggregate min = new Aggregate(seqScan, fieldIdx, Aggregator.NO_GROUPING, Aggregator.Op.MIN);
        Tuple tupleMin;
        min.open();
        tupleMin = min.next();
        System.out.println("MIN(" + col + "): " + tupleMin);
        min.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    @Test
    public void testEndToEndQueryFilter() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        // Construct the query
    	System.out.println("Original Query: SELECT a FROM end_to_end_enc_test WHERE a > 30");
    	// System.out.println("Encrypted Query: SELECT OPE_a FROM end_to_end_enc_test_enc WHERE OPE_a > 150");
        TransactionId tid = new TransactionId();
        String col = "a";
        int fieldIdx = this.table.getTupleDesc().fieldNameToIndex(col);
        int bound = 30;
        IntField intField = new IntField(bound);
        SeqScan seqScan = new SeqScan(tid, this.table.getId());
        Predicate pred = new Predicate(fieldIdx, Predicate.Op.GREATER_THAN, intField);
        Filter filter = new Filter(pred, seqScan);
        Tuple tuple;
        filter.open();
        System.out.println("Results");
        HashSet<BigInteger> results = new HashSet<BigInteger>();
        while (filter.hasNext()) {
          tuple = filter.next();
          int resultVal = ((IntField) tuple.getField(fieldIdx)).getValue();
          BigInteger result = BigInteger.valueOf(resultVal);
          if (!results.contains(result)) {
            System.out.println(result);
          }
          results.add(result);
        }
        filter.close();
        Database.getBufferPool().transactionComplete(tid);
    }
}
