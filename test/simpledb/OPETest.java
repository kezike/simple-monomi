package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import junit.framework.JUnit4TestAdapter;
import java.io.File;
import java.math.BigInteger;

public class OPETest {
    private OPE_CipherPublic.Add add_15_pub;
    private OPE_CipherPrivate.Add add_15_priv;
    private OPE_CipherPublic.Sub sub_3_pub;
    private OPE_CipherPrivate.Sub sub_3_priv;
    private OPE_CipherPublic.Mult mult_7_pub;
    private OPE_CipherPrivate.Mult mult_7_priv;
    private OPE_CipherPublic.Line line_3_1_pub;
    private OPE_CipherPrivate.Line line_3_1_priv;

    private HeapFile table;
    private DbFileIterator table_iter;

    /**
     * Initialize each OPE unit test
     */
    @Before
    public void setupOPETest() {
        this.add_15_pub = new OPE_CipherPublic.Add(BigInteger.valueOf(15));
        this.add_15_priv = new OPE_CipherPrivate.Add(BigInteger.valueOf(15));
        this.sub_3_pub = new OPE_CipherPublic.Sub(BigInteger.valueOf(3));
        this.sub_3_priv = new OPE_CipherPrivate.Sub(BigInteger.valueOf(3));
        this.mult_7_pub = new OPE_CipherPublic.Mult(BigInteger.valueOf(7));
        this.mult_7_priv = new OPE_CipherPrivate.Mult(BigInteger.valueOf(7));
        this.line_3_1_pub = new OPE_CipherPublic.Line(BigInteger.valueOf(3), BigInteger.ONE);
        this.line_3_1_priv = new OPE_CipherPrivate.Line(BigInteger.valueOf(3), BigInteger.ONE);

        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "a", "b", "c" };
        TupleDesc td = new TupleDesc(types, names);
        this.table = new HeapFile(new File("test/simpledb/ope_test.dat"), td);

        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "ope_test");
        this.table_iter = this.table.iterator(new TransactionId());
    }

    @Test
    public void testOPEAdd() throws DbException, TransactionAbortedException {
        OPE_PublicKey pub_key = new OPE_PublicKey(this.add_15_pub);
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.add_15_priv);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              BigInteger val_orig = BigInteger.valueOf(int_field.getValue());
              BigInteger val_enc = pub_key.encrypt(val_orig);
              BigInteger val_dec = key_pair.decrypt(val_enc);
              assertFalse(val_orig.equals(val_enc));
              assertFalse(val_enc.equals(val_dec));
              assertEquals(val_orig, val_dec);
            }
          }
        }
        this.table_iter.rewind();
        this.table_iter.close();
    }

    @Test
    public void testOPESub() throws DbException, TransactionAbortedException {
        OPE_PublicKey pub_key = new OPE_PublicKey(this.sub_3_pub);
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.sub_3_priv);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              BigInteger val_orig = BigInteger.valueOf(int_field.getValue());
              BigInteger val_enc = pub_key.encrypt(val_orig);
              BigInteger val_dec = key_pair.decrypt(val_enc);
              assertFalse(val_orig.equals(val_enc));
              assertFalse(val_enc.equals(val_dec));
              assertEquals(val_orig, val_dec);
            }
          }
        }
        this.table_iter.rewind();
        this.table_iter.close();
    }

    @Test
    public void testOPEMult() throws DbException, TransactionAbortedException {
        OPE_PublicKey pub_key = new OPE_PublicKey(this.mult_7_pub);
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.mult_7_priv);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              BigInteger val_orig = BigInteger.valueOf(int_field.getValue());
              BigInteger val_enc = pub_key.encrypt(val_orig);
              BigInteger val_dec = key_pair.decrypt(val_enc);
              assertFalse(val_orig.equals(val_enc));
              assertFalse(val_enc.equals(val_dec));
              assertEquals(val_orig, val_dec);
            }
          }
        }
        this.table_iter.rewind();
        this.table_iter.close();
    }

    @Test
    public void testOPELine() throws DbException, TransactionAbortedException {
        OPE_PublicKey pub_key = new OPE_PublicKey(this.line_3_1_pub);
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.line_3_1_priv);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              BigInteger val_orig = BigInteger.valueOf(int_field.getValue());
              BigInteger val_enc = pub_key.encrypt(val_orig);
              BigInteger val_dec = key_pair.decrypt(val_enc);
              assertFalse(val_orig.equals(val_enc));
              assertFalse(val_enc.equals(val_dec));
              assertEquals(val_orig, val_dec);
            }
          }
        }
        this.table_iter.rewind();
        this.table_iter.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(OPETest.class);
    }
}
