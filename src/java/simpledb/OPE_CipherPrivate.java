package simpledb;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * An interface for defining decrypt functions
 */
public interface OPE_CipherPrivate {
   public BigInteger decrypt(BigInteger val);
   
   /**
    * An addition implementation of the OPE_CipherPrivate interface
    */
   public class Add implements OPE_CipherPrivate, Serializable {
      private BigInteger cipher;

      public Add(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger decrypt(BigInteger val) {
          return val.subtract(this.cipher);
      }
   }

   /**
    * A subtraction implementation of the OPE_CipherPrivate interface
    */
   public class Sub implements OPE_CipherPrivate, Serializable {
      private BigInteger cipher;

      public Sub(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger decrypt(BigInteger val) {
          return val.add(this.cipher);
      }
   }

   /**
    * A multiplication implementation of the OPE_CipherPrivate interface
    */
   public class Mult implements OPE_CipherPrivate, Serializable {
      private BigInteger cipher;

      public Mult(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger decrypt(BigInteger val) {
          return val.divide(this.cipher);
      }
   }

   /**
    * A linear implementation of the OPE_CipherPrivate interface
    */
   public class Line implements OPE_CipherPrivate, Serializable {
      private BigInteger cipher_slope;
      private BigInteger cipher_y_int;

      public Line(BigInteger cipher_m, BigInteger cipher_b) {
          this.cipher_slope = cipher_m;
          this.cipher_y_int = cipher_b;
      }
      
      public BigInteger decrypt(BigInteger val) {
          return val.subtract(cipher_y_int).divide(cipher_slope);
      }
   }
}
