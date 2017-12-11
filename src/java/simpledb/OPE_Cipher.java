package simpledb;

/**
 * An interface for defining encrypt/decrypt functions
 */
public interface OPE_Cipher {
   public Integer encrypt(Integer val);
   public Integer decrypt(Integer val);
   
   /**
    * An addition implementation of the OPE_Cipher interface
    */
   public class Add implements OPE_Cipher {
      private Integer cipher;

      public Add(Integer cipher) {
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
   public class Sub implements OPE_Cipher {
      private Integer cipher;

      public Sub(Integer cipher) {
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
   public class Mult implements OPE_Cipher {
      private Integer cipher;

      public Mult(Integer cipher) {
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
   public class Div implements OPE_Cipher {
      private Integer cipher;

      public Div(Integer cipher) {
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
   public class Line implements OPE_Cipher {
      private Integer cipher_slope;
      private Integer cipher_y_int;

      public Line(Integer cipher_m, Integer cipher_b) {
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
}