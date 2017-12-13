package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;
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
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(EncryptionTest.class);
    }
}
