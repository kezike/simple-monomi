package simpledb;

import java.math.BigInteger;
import java.util.Random;

/**
 * A class that represents the public part of the OPE key pair.
 * 
 */
public class OPE_PublicKey {
    private OPE ope;

    OPE_PublicKey(OPE ope) {
        this.ope = ope;
    }

    public OPE getOPE() {
        return this.ope;
    }

    /**
     * Encrypts the given plaintext.
     *
     * @param val The plaintext that should be encrypted.
     * @return The corresponding ciphertext.
     */
    public final Integer encrypt(Integer val) {
        return this.ope.encrypt(val);
    }
}
