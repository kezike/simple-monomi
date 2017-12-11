package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = hf.getId();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.getId());
        assertEquals(id, hf.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {    	
        assertEquals(td, hf.getTupleDesc());        
    }
    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() throws Exception {
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
        assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.isSlotUsed(1));
        assertFalse(page.isSlotUsed(20));
    }

    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }
        assertEquals(3, count);
        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);

        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }
        // close twice is harmless
        it.close();
    }

    @Test
    public void encrypt() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);
        EncryptedFile encF = smallFile.encrypt();

        // Private key should be encrypted somewhere, should get it
        
        
        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        KeyPair keyPairFromFile;
        try {
            String filename = String.valueOf(smallFile.getId()) + ".paillier";
            ObjectInputStream objectInputStream = 
                    new ObjectInputStream(new FileInputStream(filename));
            keyPairFromFile = (KeyPair) objectInputStream.readObject();
            objectInputStream.close();
            
            DbFileIterator encIt = encF.iterator(tid);

            it.open();
            encIt.open();
            
            int count = 0;
            System.out.println("Iterating through small file");
            while (it.hasNext() && encIt.hasNext()) {
                Tuple tup = it.next();
                Tuple encTup = encIt.next();
                assertNotNull(tup);
                assertNotNull(encTup);
                count += 1;
                System.out.println("\nOriginal tuple: " + tup);
                System.out.println("Encrypted: " + encTup);
                assertFalse(tup.equals(encTup));
                
                System.out.println("New pub: " + keyPairFromFile.getPublicKey().toString());
                System.out.println("New priv: " + keyPairFromFile.getPrivateKey().toString());
                
                // now decrypt the first 2 columns which should be Paillier
                int firstEncVal = ((IntField) encTup.getField(0)).getValue();
                BigInteger firstEncNum = BigInteger.valueOf(firstEncVal);
                BigInteger decrypted = keyPairFromFile.decrypt(firstEncNum);
                int firstOrigVal = ((IntField) tup.getField(0)).getValue();
                System.out.println("Expected: " + firstOrigVal + " Actual: " + decrypted.toString());
                assertEquals(BigInteger.valueOf(firstOrigVal), decrypted);
                
                int secondEncVal = ((IntField) encTup.getField(1)).getValue();
                BigInteger secondNum = BigInteger.valueOf(secondEncVal);
                decrypted = keyPairFromFile.decrypt(secondNum);
//                System.out.println("Expected: " + secondNum + " Actual: " + decrypted.toString());
//                assertEquals(BigInteger.valueOf(secondEncVal), decrypted);
                                
            }
            assertEquals(3, count);
            it.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail("Reading of public key failed.");
        }        
        
    }

    
    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
