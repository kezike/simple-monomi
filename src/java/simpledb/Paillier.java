package simpledb;

import java.math.BigInteger;

/**
 * Static class to provide some convenience methods for Paillier operations
 * @author Carlos Henriquez
 *
 */
public class Paillier {

    public Paillier() {

    }

    /**
     * Takes 2 encrypted BigIntegers and returns their Paillier sum, which is
     *  the multiplication of the encrypted values mod the n squared of the public key
     * @param A an encrypted BigInt
     * @param B an encrypted BigInt
     * @param P the PublicKey used to encrypt A and B 
     * @return their Paillier sum
     */
    public static BigInteger add(BigInteger encA, BigInteger encB, PublicKey P) {
        return encA.multiply(encB).mod(P.getnSquared());
    }
    
    /**
     * Takes an encrypted value A and returns the encrypted result of multiplying it by a 
     * constant A
     * @param encA
     * @param B
     * @param P the PublicKey used to encrypt encryptedA
     * @return The product
     */
    public static BigInteger constMult(BigInteger encA, BigInteger B, PublicKey P) {
        return encA.modPow(B, P.getnSquared());
    }
    
}
