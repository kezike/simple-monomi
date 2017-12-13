package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;
import simpledb.systemtest.SystemTestUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

public class EncryptionTest {
    
    private HeapFile table;

    /**
     * Initialize each OPE unit test
     */
    @Before
    public void setupEncryptionTest() {
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "a", "b", "c" };
        TupleDesc td = new TupleDesc(types, names);
        this.table = new HeapFile(new File("test/simpledb/ope_test.dat"), td);
        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "ope_test");
    }

    @Test
    public void testEncrypt() throws IOException, DbException, TransactionAbortedException {
        long startTime = System.nanoTime();
        ConcurrentHashMap<String, KeyPair> keyPairs = new ConcurrentHashMap<String, KeyPair>();
        
        Paillier_KeyPairBuilder paillierKeyGen = new Paillier_KeyPairBuilder();
        paillierKeyGen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        paillierKeyGen.bits(HeapFile.BITS_INTEGER);
        Paillier_KeyPair paillerKeyPair = paillierKeyGen.generateKeyPair();
        
        OPE_CipherPrivate opeCipherPrivate = new OPE_CipherPrivate.Mult(BigInteger.valueOf(5));
        OPE_CipherPublic opeCipherPublic = new OPE_CipherPublic.Mult(BigInteger.valueOf(5));
        OPE_PrivateKey opePrivateKey = new OPE_PrivateKey(opeCipherPrivate);
        OPE_PublicKey opePublicKey = new OPE_PublicKey(opeCipherPublic);
        OPE_KeyPair opeKeyPair = new OPE_KeyPair(opePrivateKey, opePublicKey);
        
        keyPairs.put(HeapFile.PAILLIER_PREFIX, (KeyPair) paillerKeyPair);
        keyPairs.put(HeapFile.OPE_PREFIX, (KeyPair) opeKeyPair);
        EncryptedFile tableEnc = this.table.encrypt(keyPairs);
        long encryptTime = System.nanoTime();
        System.out.println("Time to encrypt file with 3 rows: " + (encryptTime - startTime)/1000000 + " ms");
        
        TupleDesc tupDescEnc = tableEnc.getTupleDesc();
        assertEquals(this.table.getTupleDesc().numFields() * HeapFile.NUM_ENCRYPTIONS + HeapFile.NUM_EXTRA_COLUMNS, tupDescEnc.numFields());
        
        
        
        DbFileIterator plainTableEncIter = this.table.iterator(new TransactionId());
        DbFileIterator tableEncIter = tableEnc.iterator(new TransactionId());
        tableEncIter.open();
        plainTableEncIter.open();

        Tuple tupleEnc;
        Tuple tuplePlain;
        // Testing first-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values        
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(20, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(50, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(30, ((IntField) tupleEnc.getField(5)).getValue());
        
        // Testing second-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(35, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(5, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(75, ((IntField) tupleEnc.getField(5)).getValue());
        
        // Testing third-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(10, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(25, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(40, ((IntField) tupleEnc.getField(5)).getValue());
        
        tableEncIter.close();
        plainTableEncIter.open();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println("Test runtime: " + duration/1000000 + " ms");
    }
    /*
    @Test
    public void testLongEncrypt() throws Exception {
        long million = 1000000;
        long startTime = System.nanoTime();
        ConcurrentHashMap<String, KeyPair> keyPairs = new ConcurrentHashMap<String, KeyPair>();
        
        Paillier_KeyPairBuilder paillierKeyGen = new Paillier_KeyPairBuilder();
        paillierKeyGen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        paillierKeyGen.bits(HeapFile.BITS_INTEGER);
        Paillier_KeyPair paillerKeyPair = paillierKeyGen.generateKeyPair();
        
        OPE_CipherPrivate opeCipherPrivate = new OPE_CipherPrivate.Mult(BigInteger.valueOf(5));
        OPE_CipherPublic opeCipherPublic = new OPE_CipherPublic.Mult(BigInteger.valueOf(5));
        OPE_PrivateKey opePrivateKey = new OPE_PrivateKey(opeCipherPrivate);
        OPE_PublicKey opePublicKey = new OPE_PublicKey(opeCipherPublic);
        OPE_KeyPair opeKeyPair = new OPE_KeyPair(opePrivateKey, opePublicKey);
        
        keyPairs.put(HeapFile.PAILLIER_PREFIX, (KeyPair) paillerKeyPair);
        keyPairs.put(HeapFile.OPE_PREFIX, (KeyPair) opeKeyPair);
       
        long keyGenTime = System.nanoTime();
        System.out.println("Keygen time: " + (keyGenTime - startTime)/million + " ms");
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(10000, 100, null,
                null);
        long fileTime = System.nanoTime();
        System.out.println("File creation time: " + (fileTime - keyGenTime)/million + " ms");
        EncryptedFile encF = smallFile.encrypt();
        
        
        EncryptedFile tableEnc = this.table.encrypt(keyPairs);
        long encryptTime = System.nanoTime();
        System.out.println("Time to encrypt file with 10000 rows + 100 columns: " + (encryptTime - startTime)/million + " ms");
        
        TupleDesc tupDescEnc = tableEnc.getTupleDesc();
        assertEquals(this.table.getTupleDesc().numFields() * HeapFile.NUM_ENCRYPTIONS + HeapFile.NUM_EXTRA_COLUMNS, tupDescEnc.numFields());
        
        
        
        DbFileIterator plainTableEncIter = this.table.iterator(new TransactionId());
        DbFileIterator tableEncIter = tableEnc.iterator(new TransactionId());
        tableEncIter.open();
        plainTableEncIter.open();

        Tuple tupleEnc;
        Tuple tuplePlain;
        // Testing first-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values        
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(20, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(50, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(30, ((IntField) tupleEnc.getField(5)).getValue());
        
        // Testing second-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(35, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(5, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(75, ((IntField) tupleEnc.getField(5)).getValue());
        
        // Testing third-row encrypted values
        tupleEnc = tableEncIter.next();
        tuplePlain = plainTableEncIter.next();
        // Paillier values
        assertEquals(((IntField) tuplePlain.getField(0)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(0)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(1)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(1)).getValue()).intValue());
        assertEquals(((IntField) tuplePlain.getField(2)).getValue(), paillerKeyPair.decrypt(((BigIntField) tupleEnc.getField(2)).getValue()).intValue());
        // OPE values
        assertEquals(10, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(25, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(40, ((IntField) tupleEnc.getField(5)).getValue());
        
        tableEncIter.close();
        plainTableEncIter.open();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println("Test runtime: " + duration/million + " ms");

    }
*/
    @Test
    public void testMediumEncrypt() throws Exception {
        long million = 1000000;
        int rows = 10;
        int cols = 10;
        long startTime = System.nanoTime();
        ConcurrentHashMap<String, KeyPair> keyPairs = new ConcurrentHashMap<String, KeyPair>();
        
        Paillier_KeyPairBuilder paillierKeyGen = new Paillier_KeyPairBuilder();
        paillierKeyGen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        paillierKeyGen.bits(HeapFile.BITS_INTEGER);
        Paillier_KeyPair paillerKeyPair = paillierKeyGen.generateKeyPair();
        
        OPE_CipherPrivate opeCipherPrivate = new OPE_CipherPrivate.Mult(BigInteger.valueOf(5));
        OPE_CipherPublic opeCipherPublic = new OPE_CipherPublic.Mult(BigInteger.valueOf(5));
        OPE_PrivateKey opePrivateKey = new OPE_PrivateKey(opeCipherPrivate);
        OPE_PublicKey opePublicKey = new OPE_PublicKey(opeCipherPublic);
        OPE_KeyPair opeKeyPair = new OPE_KeyPair(opePrivateKey, opePublicKey);
        
        keyPairs.put(HeapFile.PAILLIER_PREFIX, (KeyPair) paillerKeyPair);
        keyPairs.put(HeapFile.OPE_PREFIX, (KeyPair) opeKeyPair);
       
        long keyGenTime = System.nanoTime();
        System.out.println("Keygen time: " + (keyGenTime - startTime)/million + " ms");
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(rows, cols, null,
                null);
        long fileTime = System.nanoTime();
        System.out.println("File creation time: " + (fileTime - keyGenTime)/million + " ms");
        EncryptedFile encF = smallFile.encrypt();
        
        
        EncryptedFile tableEnc = this.table.encrypt(keyPairs);
        long encryptTime = System.nanoTime();
        System.out.println("Time to encrypt file with " +rows+" rows "+cols+" 10 columns: " + (encryptTime - fileTime)/million + " ms");
        
        TupleDesc tupDescEnc = tableEnc.getTupleDesc();
        assertEquals(this.table.getTupleDesc().numFields() * HeapFile.NUM_ENCRYPTIONS + HeapFile.NUM_EXTRA_COLUMNS, tupDescEnc.numFields());
        
       
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println("Test "+rows+" "+cols+" runtime: " + duration/million + " ms\n");

    }

    public void encryptNRowsMColumns(int rows, int cols) throws Exception {
        long million = 1000000;
//        int rows = 10;
//        int cols = 10;
        long startTime = System.nanoTime();
        ConcurrentHashMap<String, KeyPair> keyPairs = new ConcurrentHashMap<String, KeyPair>();
        
        Paillier_KeyPairBuilder paillierKeyGen = new Paillier_KeyPairBuilder();
        paillierKeyGen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        paillierKeyGen.bits(HeapFile.BITS_INTEGER);
        Paillier_KeyPair paillerKeyPair = paillierKeyGen.generateKeyPair();
        
        OPE_CipherPrivate opeCipherPrivate = new OPE_CipherPrivate.Mult(BigInteger.valueOf(5));
        OPE_CipherPublic opeCipherPublic = new OPE_CipherPublic.Mult(BigInteger.valueOf(5));
        OPE_PrivateKey opePrivateKey = new OPE_PrivateKey(opeCipherPrivate);
        OPE_PublicKey opePublicKey = new OPE_PublicKey(opeCipherPublic);
        OPE_KeyPair opeKeyPair = new OPE_KeyPair(opePrivateKey, opePublicKey);
        
        keyPairs.put(HeapFile.PAILLIER_PREFIX, (KeyPair) paillerKeyPair);
        keyPairs.put(HeapFile.OPE_PREFIX, (KeyPair) opeKeyPair);
       
        long keyGenTime = System.nanoTime();
        System.out.println("Keygen time: " + (keyGenTime - startTime)/million + " ms");
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(rows, cols, null,
                null);
        long fileTime = System.nanoTime();
        System.out.println("File creation time: " + (fileTime - keyGenTime)/million + " ms");
        EncryptedFile encF = smallFile.encrypt();
        
        
        EncryptedFile tableEnc = this.table.encrypt(keyPairs);
        long encryptTime = System.nanoTime();
        System.out.println("Time to encrypt file with " +rows+" rows "+cols+" columns: " + (encryptTime - fileTime)/million + " ms");
        
        TupleDesc tupDescEnc = tableEnc.getTupleDesc();
        assertEquals(this.table.getTupleDesc().numFields() * HeapFile.NUM_ENCRYPTIONS + HeapFile.NUM_EXTRA_COLUMNS, tupDescEnc.numFields());
        
       
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println("Test "+rows+" "+cols+" runtime: " + duration/million + " ms\n");

    }

   /* @Test
    public void  testEncryptMany() throws Exception {
        encryptNRowsMColumns(1,1);
        encryptNRowsMColumns(10,1);
        encryptNRowsMColumns(1,10);
        encryptNRowsMColumns(10,10);
        encryptNRowsMColumns(1,100);
        encryptNRowsMColumns(100,1);
        //encryptNRowsMColumns(100,10);
        //encryptNRowsMColumns(10,100);
        
    }*/
    
    @Test
    public void test1r10c() throws Exception{
        encryptNRowsMColumns(1,10);   
    }

    @Test
    public void test10r1c() throws Exception{
        encryptNRowsMColumns(10,1);   
    }
    @Test
    public void test10r10c() throws Exception{
        encryptNRowsMColumns(10,10);   
    }
    @Test
    public void test1r100c() throws Exception{
        encryptNRowsMColumns(1,100);   
    }

    @Test
    public void test100r10c() throws Exception{
        encryptNRowsMColumns(100,1);   
    }
    
    @Test
    public void test20r20c() throws Exception{
        encryptNRowsMColumns(20,20);   
    }

    /*@Test
    public void test100r100c() throws Exception{
        encryptNRowsMColumns(100,100);   
    }*/
    
    /*@Test
    public void test1000r100c() throws Exception{
        encryptNRowsMColumns(1000,100);   
    }*/
    
    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(EncryptionTest.class);
    }
}
