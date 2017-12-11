package simpledb;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class OPE {

  public enum ENC_MODE implements Serializable {
      ADD, MULT, POWER, CRYPTO;
  }

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

      public void set_value(Integer val) {
          this.value = val;
      }

      public Integer get_value() {
          return this.value;
      }

      public void set_less(OPE_Node less) {
          this.less = less;
      }

      public OPE_Node get_less() {
          return this.less;
      }

      public void set_more(OPE_Node more) {
          this.more = more;
      }

      public OPE_Node get_more() {
          return this.more;
      }
  }

  private ENC_MODE enc_mode;
  private Integer cipher;
  private OPE_Node ope_node;
  private ConcurrentHashMap<Integer, Integer> encryption_map;

  public OPE(ENC_MODE enc_m, Integer ciph) {
      this.enc_mode = enc_m;
      this.cipher = ciph;
      this.ope_node = new OPE_Node();
      this.encryption_map = new ConcurrentHashMap<Integer, Integer>();
  }
  
  private ArrayList<Integer> sort(HeapFile hf, int col) {
      HashSet<Integer> values = new ArrayList<Integer>();
      DbFileIterator hf_iter = hf.iterator(new TransactionId());
      hf_iter.open();
      while (hf_iter.hasNext()) {
        Tuple tuple = hf_iter.next();
        Field field = next.getField(col);
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

  public void encrypt(Integer val) {
      Integer encrypted_value;
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
  }  
  
  private OPE_Node build_ope_tree(ArrayList<Integer> values, int start_idx, int end_idx) {
      if (start_idx > end_idx) {
        return null;
      }
      int mid_idx = start_idx + (end_idx - start_idx) / 2;
      Integer mid_val = values.get(mid_idx);
      OPE_Node node = new OPE_Node(mid_val);
      OPE_Node less = this.build_ope_tree(values, start_idx, mid_idx - 1);
      OPE_Node more = this.build_ope_tree(values, mid_idx + 1, end_idx);
      node.set_less(less);
      node.set_more(more);
      return node;
  }

  public OPE_Node encrypt(HeapFile file, int col) {
      ArrayList<Integer> values = this.sort(file, col);
      int start_idx = 0;
      int end_idx = values.size() - 1
      OPE_Node root = this.build_ope_tree(values, start_idx, end_idx);
      return root;
  }
}
