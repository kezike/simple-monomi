package simpledb;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

/**
 * A class that represents the public part of the Paillier key pair.
 * <p>
 * As in all asymmetric cryptographic systems it is responsible for the
 * encryption.
 * <p>
 * Additional instructions for the decryption can be found on {@link Paillier_KeyPair}.
 *
 * Borrowed from https://github.com/kunerd/jpaillier
 * @see Paillier_KeyPair
 */
public class Paillier_PublicKey implements PublicKey, Serializable {
    private final int bits;
    private final BigInteger n;
    private final BigInteger nSquared;
    private final BigInteger g;

    Paillier_PublicKey(BigInteger n, BigInteger nSquared, BigInteger g, int bits) {
        this.n = n;
        this.nSquared = nSquared;
        this.bits = bits;
        this.g = g;
    }

    public int getBits() {
        return bits;
    }

    public BigInteger getN() {
        return n;
    }

    public BigInteger getnSquared() {
        return nSquared;
    }

    public BigInteger getG() {
        return g;
    }

    /**
     * Encrypts the given plaintext.
     *
     * @param m The plaintext that should be encrypted.
     * @return The corresponding ciphertext.
     */
    public final BigInteger encrypt(BigInteger m) {

        BigInteger r;
        do {
            r = new BigInteger(bits, new Random());
        } while (r.compareTo(n) >= 0);

        BigInteger result = g.modPow(m, nSquared);
        BigInteger x = r.modPow(n, nSquared);

        result = result.multiply(x);
        result = result.mod(nSquared);

        return result;
    }
    
    public boolean equals(Object p) {
    		Paillier_PublicKey publicKey = (Paillier_PublicKey) p;
        return (this.getBits() == publicKey.getBits()) &&
        		(this.getN().compareTo(publicKey.getN()) == 0) &&
        		(this.getG().compareTo(publicKey.getG()) == 0) &&
        		(this.getnSquared().compareTo(publicKey.getnSquared()) == 0);
    }
    
    public String toString() {
    return "Bits: " + getBits() + " N: " + getN() + " G: " + getG() + " N^2: " + getnSquared();
}
    
}