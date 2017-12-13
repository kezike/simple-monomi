package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Collections;
import java.math.BigInteger;
// import java.util.concurrent.ConcurrentHashMap;

public class OPE {
    public class OPE_Node {
        private BigInteger value;
        private OPE_Node less;
        private OPE_Node more;

        public OPE_Node(BigInteger val) {
            this.value = val;
            this.less = null;
            this.more = null;
        }

        public OPE_Node() {
            this.value = null;
            this.less = null;
            this.more = null;
        }

        public void setValue(BigInteger val) {
            this.value = val;
        }

        public BigInteger getValue() {
            return this.value;
        }

        public void setLess(OPE_Node less) {
            this.less = less;
        }

        public OPE_Node getLess() {
            return this.less;
        }

        public void setMore(OPE_Node more) {
            this.more = more;
        }

        public OPE_Node getMore() {
            return this.more;
        }
    }
    
    private OPE_Node ope_node;

    public OPE() {
        this.ope_node = new OPE_Node();
    }

    public static BigInteger max(BigInteger int1, BigInteger int2) {
        // TODO: this only works if integers are mapped to integers;
        // if mapped to strings, will have to read OPE_Node
        return int1.max(int2);
    }

    public static BigInteger min(BigInteger int1, BigInteger int2) {
        // TODO: this only works if integers are mapped to integers;
        // if mapped to strings, will have to read OPE_Node
        return int1.min(int2);
    }
    
    private ArrayList<BigInteger> sort(HeapFile hf, int col) throws DbException, TransactionAbortedException {
        HashSet<BigInteger> values = new HashSet<BigInteger>();
        DbFileIterator hf_iter = hf.iterator(new TransactionId());
        hf_iter.open();
        while (hf_iter.hasNext()) {
          Tuple tuple = hf_iter.next();
          Field field = tuple.getField(col);
          if (field.getType() == Type.INT_TYPE) {
            IntField int_field = (IntField) field;
            int int_val = int_field.getValue();
            values.add(BigInteger.valueOf(int_val));
          }
        }
        hf_iter.close();
        ArrayList<BigInteger> values_list = new ArrayList(values);
        Collections.sort(values_list);
        return values_list;
    }

    private OPE_Node buildOPETree(ArrayList<BigInteger> values, int start_idx, int end_idx) {
        if (start_idx > end_idx) {
          return null;
        }
        int mid_idx = start_idx + (end_idx - start_idx) / 2;
        BigInteger mid_val = values.get(mid_idx);
        OPE_Node node = new OPE_Node(mid_val);
        OPE_Node less = this.buildOPETree(values, start_idx, mid_idx - 1);
        OPE_Node more = this.buildOPETree(values, mid_idx + 1, end_idx);
        node.setLess(less);
        node.setMore(more);
        return node;
    }

    public OPE_Node encrypt(HeapFile file, int col, OPE_CipherPublic cipher) throws DbException, TransactionAbortedException {
        ArrayList<BigInteger> values = this.sort(file, col);
        ArrayList<BigInteger> values_enc = new ArrayList<BigInteger>();
        for (BigInteger value : values) {
          values_enc.add(cipher.encrypt(value));
        }
        int start_idx = 0;
        int end_idx = values.size() - 1;
        OPE_Node root = this.buildOPETree(values_enc, start_idx, end_idx);
        return root;
    }
}
