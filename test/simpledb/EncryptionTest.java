package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import junit.framework.JUnit4TestAdapter;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

public class EncryptionTest {
    
    private HeapFile table;
    private DbFileIterator tableIter;

    /**
     * Initialize each OPE unit test
     */
    @Before
    public void setupEncryptionTest() {
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc td = new TupleDesc(types, names);
        this.table = new HeapFile(new File("test/simpledb/ope_test.dat"), td);
        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "ope_test");
        this.tableIter = this.table.iterator(new TransactionId());
    }

    @Test
    public void testEncrypt() throws IOException, DbException, TransactionAbortedException {
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
        TupleDesc tupDescEnc = tableEnc.getTupleDesc();
        assertEquals(this.table.getTupleDesc().numFields() * HeapFile.NUM_ENCRYPTIONS + HeapFile.NUM_EXTRA_COLUMNS, tupDescEnc.numFields());
        DbFileIterator tableEncIter = tableEnc.iterator(new TransactionId());
        tableEncIter.open();
        Tuple tupleEnc;
        // Testing first-row OPE values
        tupleEnc = tableEncIter.next();
        assertEquals(20, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(50, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(30, ((IntField) tupleEnc.getField(5)).getValue());
        // Testing second-row OPE values
        tupleEnc = tableEncIter.next();
        assertEquals(35, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(5, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(75, ((IntField) tupleEnc.getField(5)).getValue());
        // Testing third-row OPE values
        tupleEnc = tableEncIter.next();
        assertEquals(10, ((IntField) tupleEnc.getField(3)).getValue());
        assertEquals(25, ((IntField) tupleEnc.getField(4)).getValue());
        assertEquals(40, ((IntField) tupleEnc.getField(5)).getValue());
        tableEncIter.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(EncryptionTest.class);
    }
}
