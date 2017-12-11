package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class OPE {
    /*public enum ENC_MODE implements Serializable {
        ADD, MULT, POWER, CRYPTO;
    }*/

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

    /**
      * An interface for defining encrypt/decrypt functions
      */
    public interface OPE_Cipher {
        public Integer encrypt(Integer val);
        public Integer decrypt(Integer val);
    }

    /**
      * An addition implementation of the OPE_Cipher interface
      */
    public class OPE_Add implements OPE_Cipher {
        private Integer cipher;

        public OPE_Add(Integer cipher) {
            this.cipher = cipher;
        }
        
        public Integer encrypt(Integer val) {
            return val + this.cipher;
        }
        
        public Integer decrypt(Integer val) {
            return val - this.cipher;
        }
    }

    /**
      * A subtraction implementation of the OPE_Cipher interface
      */
    public class OPE_Sub implements OPE_Cipher {
        private Integer cipher;

        public OPE_Sub(Integer cipher) {
            this.cipher = cipher;
        }
        
        public Integer encrypt(Integer val) {
            return val - this.cipher;
        }
        
        public Integer decrypt(Integer val) {
            return val + this.cipher;
        }
    }

    /**
      * A multiplication implementation of the OPE_Cipher interface
      */
    public class OPE_Mult implements OPE_Cipher {
        private Integer cipher;

        public OPE_Mult(Integer cipher) {
            this.cipher = cipher;
        }
        
        public Integer encrypt(Integer val) {
            return val * this.cipher;
        }
        
        public Integer decrypt(Integer val) {
            return val / this.cipher;
        }
    }

    /**
      * A division implementation of the OPE_Cipher interface
      */
    public class OPE_Div implements OPE_Cipher {
        private Integer cipher;

        public OPE_Div(Integer cipher) {
            this.cipher = cipher;
        }
        
        public Integer encrypt(Integer val) {
            return val / this.cipher;
        }
        
        public Integer decrypt(Integer val) {
            return val * this.cipher;
        }
    }


    /**
      * A linear implementation of the OPE_Cipher interface
      */
    public class OPE_Line implements OPE_Cipher {
        private Integer cipher_slope;
        private Integer cipher_y_int;

        public OPE_Line(Integer cipher_m, Integer cipher_b) {
            this.cipher_slope = cipher_m;
            this.cipher_y_int = cipher_b;
        }
        
        public Integer encrypt(Integer val) {
            return this.cipher_slope * val + cipher_y_int;
        }
        
        public Integer decrypt(Integer val) {
            return (val - cipher_y_int) / cipher_slope;
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

    /*public Integer encrypt(Integer val) {
        Integer encrypted_value = new Integer(0);
        if (this.enc_mode == ENC_MODE.ADD) {
          encrypted_value = val + this.cipher;
          encryption_map.put(val, encrypted_value);
        } else if (this.enc_mode == ENC_MODE.MULT) {
          encrypted_value = val * this.cipher;
          encryption_map.put(val, encrypted_value);
        } else if (this.enc_mode == ENC_MODE.POWER) {
          // TODO
        } else if (this.enc_mode == ENC_MODE.CRYPTO) {
          // TODO
        }
        return encrypted_value;
    }
    
    public Integer decrypt(Integer val) {
        Integer decrypted_value = new Integer(0);
        if (this.enc_mode == ENC_MODE.ADD) {
          decrypted_value = val - this.cipher;
        } else if (this.enc_mode == ENC_MODE.MULT) {
          decrypted_value = val / this.cipher;
        } else if (this.enc_mode == ENC_MODE.POWER) {
          // TODO
        } else if (this.enc_mode == ENC_MODE.CRYPTO) {
          // TODO
        }
        return decrypted_value;
    }*/
    
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
