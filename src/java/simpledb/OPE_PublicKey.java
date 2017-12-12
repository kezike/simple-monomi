package simpledb;

import java.math.BigInteger;
import java.util.Random;

/**
 * A class that represents the public part of the OPE key pair.
 * 
 */
public class OPE_PublicKey {
    private OPE_CipherPublic cipher;

    OPE_PublicKey(OPE_CipherPublic cipher) {
        this.cipher = cipher;
    }

    public OPE_CipherPublic getCipher() {
        return this.cipher;
    }

    /**
     * Encrypts the given plaintext.
     *
     * @param val The plaintext that should be encrypted.
     * @return The corresponding ciphertext.
     */
    public final Integer encrypt(Integer val) {
        return this.cipher.encrypt(val);
    }
}
