package simpledb;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import junit.framework.JUnit4TestAdapter;
import java.io.File;

public class OPETest {
    private OPE_Cipher.Add add_15;
    private OPE_Cipher.Sub sub_3;
    private OPE_Cipher.Mult mult_7;
    private OPE_Cipher.Line line_3_1;

    private OPE ope_add_15;
    private OPE ope_sub_3;
    private OPE ope_mult_7;
    private OPE ope_line_3_1;

    private HeapFile table;
    private DbFileIterator table_iter;

    /**
     * Initialize each OPE unit test
     */
    @Before
    public void setupOPETest() throws Exception {
        this.add_15 = new OPE_Cipher.Add(15);
        this.sub_3 = new OPE_Cipher.Sub(3);
        this.mult_7 = new OPE_Cipher.Mult(7);
        this.line_3_1 = new OPE_Cipher.Line(3, 1);

        this.ope_add_15 = new OPE(add_15);
        this.ope_sub_3 = new OPE(sub_3);
        this.ope_mult_7 = new OPE(mult_7);
        this.ope_line_3_1 = new OPE(line_3_1);

        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc td = new TupleDesc(types, names);
        this.table = new HeapFile(new File("test/simpledb/ope_test.dat"), td);
        Catalog catalog = Database.getCatalog();
        catalog.addTable(this.table, "ope_test");
        this.table_iter = this.table.iterator(new TransactionId());
    }

    @Test
    public void testOPEAdd() throws DbException, TransactionAbortedException {
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.ope_add_15);
        OPE_PublicKey pub_key = new OPE_PublicKey(this.ope_add_15);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key, this.ope_add_15);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              Integer val_orig = int_field.getValue();
              Integer val_enc = pub_key.encrypt(val_orig);
              Integer val_dec = key_pair.decrypt(val_enc);
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
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.ope_sub_3);
        OPE_PublicKey pub_key = new OPE_PublicKey(this.ope_sub_3);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key, this.ope_sub_3);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              Integer val_orig = int_field.getValue();
              Integer val_enc = pub_key.encrypt(val_orig);
              Integer val_dec = key_pair.decrypt(val_enc);
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
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.ope_mult_7);
        OPE_PublicKey pub_key = new OPE_PublicKey(this.ope_mult_7);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key, this.ope_mult_7);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              Integer val_orig = int_field.getValue();
              Integer val_enc = pub_key.encrypt(val_orig);
              Integer val_dec = key_pair.decrypt(val_enc);
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
        OPE_PrivateKey priv_key = new OPE_PrivateKey(this.ope_line_3_1);
        OPE_PublicKey pub_key = new OPE_PublicKey(this.ope_line_3_1);
        OPE_KeyPair key_pair = new OPE_KeyPair(priv_key, pub_key, this.ope_line_3_1);
        this.table_iter.open();
        while (this.table_iter.hasNext()) {
          Tuple tuple = this.table_iter.next();
          TupleDesc tuple_desc = tuple.getTupleDesc();
          int num_fields = tuple_desc.numFields();
          for (int i = 0; i < num_fields; i++) {
            Field field = tuple.getField(i);
            if (field.getType() == Type.INT_TYPE) {
              IntField int_field = (IntField) field;
              Integer val_orig = int_field.getValue();
              Integer val_enc = pub_key.encrypt(val_orig);
              Integer val_dec = key_pair.decrypt(val_enc);
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
