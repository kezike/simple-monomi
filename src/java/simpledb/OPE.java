package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class OPE {
    public class OPE_Node {
        private Integer value;
        private OPE_Node less;
        private OPE_Node more;

        public OPE_Node(Integer val) {
            this.value = val;
            this.less = null;
            this.more = null;
        }

        public OPE_Node() {
            this.value = null;
            this.less = null;
            this.more = null;
        }

        public void setValue(Integer val) {
            this.value = val;
        }

        public Integer getValue() {
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
    
    private OPE_Cipher cipher;
    private OPE_Node ope_node;
    private ConcurrentHashMap<Integer, Integer> encryption_map;
    private ConcurrentHashMap<Integer, Integer> decryption_map;

    public OPE(OPE_Cipher cipher) {
        this.cipher = cipher;
        this.ope_node = new OPE_Node();
        this.encryption_map = new ConcurrentHashMap<Integer, Integer>();
    }
    
    private ArrayList<Integer> sort(HeapFile hf, int col) throws DbException, TransactionAbortedException {
        HashSet<Integer> values = new HashSet<Integer>();
        DbFileIterator hf_iter = hf.iterator(new TransactionId());
        hf_iter.open();
        while (hf_iter.hasNext()) {
          Tuple tuple = hf_iter.next();
          Field field = tuple.getField(col);
          if (field.getType() == Type.INT_TYPE) {
            IntField int_field = (IntField) field;
            int int_val = int_field.getValue();
            values.add(new Integer(int_val));
          }
        }
        hf_iter.close();
        ArrayList<Integer> values_list = new ArrayList(values);
        Collections.sort(values_list);
        return values_list;
    }

    public Integer encrypt(Integer val) {
        return this.cipher.encrypt(val);
    }

    public Integer decrypt(Integer val) {
        return this.cipher.decrypt(val);
    }
    
    private OPE_Node buildOPETree(ArrayList<Integer> values, int start_idx, int end_idx) {
        if (start_idx > end_idx) {
          return null;
        }
        int mid_idx = start_idx + (end_idx - start_idx) / 2;
        Integer mid_val = values.get(mid_idx);
        OPE_Node node = new OPE_Node(mid_val);
        OPE_Node less = this.buildOPETree(values, start_idx, mid_idx - 1);
        OPE_Node more = this.buildOPETree(values, mid_idx + 1, end_idx);
        node.setLess(less);
        node.setMore(more);
        return node;
    }

    public OPE_Node encrypt(HeapFile file, int col) throws DbException, TransactionAbortedException {
        ArrayList<Integer> values = this.sort(file, col);
        int start_idx = 0;
        int end_idx = values.size() - 1;
        OPE_Node root = this.buildOPETree(values, start_idx, end_idx);
        return root;
    }
}
