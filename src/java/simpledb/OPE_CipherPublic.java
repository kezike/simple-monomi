package simpledb;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * An interface for defining encrypt functions
 */
public interface OPE_CipherPublic {
   public BigInteger encrypt(BigInteger val);
   
   /**
    * An addition implementation of the OPE_CipherPublic interface
    */
   public class Add implements OPE_CipherPublic, Serializable {
      private BigInteger cipher;

      public Add(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger encrypt(BigInteger val) {
          return val.add(this.cipher);
      }
   }

   /**
    * A subtraction implementation of the OPE_CipherPublic interface
    */
   public class Sub implements OPE_CipherPublic, Serializable {
      private BigInteger cipher;

      public Sub(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger encrypt(BigInteger val) {
          return val.subtract(this.cipher);
      }
   }

   /**
    * A multiplication implementation of the OPE_CipherPublic interface
    */
   public class Mult implements OPE_CipherPublic, Serializable {
      private BigInteger cipher;

      public Mult(BigInteger cipher) {
          this.cipher = cipher;
      }
      
      public BigInteger encrypt(BigInteger val) {
          return val.multiply(this.cipher);
      }
   }

   /**
    * A linear implementation of the OPE_CipherPublic interface
    */
   public class Line implements OPE_CipherPublic, Serializable {
      private BigInteger cipher_slope;
      private BigInteger cipher_y_int;

      public Line(BigInteger cipher_m, BigInteger cipher_b) {
          this.cipher_slope = cipher_m;
          this.cipher_y_int = cipher_b;
      }
      
      public BigInteger encrypt(BigInteger val) {
          return this.cipher_slope.multiply(val).add(cipher_y_int);
      }
   }
}
