package simpledb;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * A class that represents the private part of the OPE key pair.
 * 
 */
class OPE_PrivateKey implements PrivateKey, Serializable {
    private final OPE_CipherPrivate cipher;

    OPE_PrivateKey(OPE_CipherPrivate cipher) {
        this.cipher = cipher;
    }

    public OPE_CipherPrivate getCipher() {
        return this.cipher;
    }

    /**
     * Decrypts the given plaintext.
     *
     * @param val The plaintext that should be decrypted.
     * @return The corresponding ciphertext.
     */
    public final BigInteger decrypt(BigInteger val) {
        return this.cipher.decrypt(val);
    }
}
