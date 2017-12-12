package simpledb;

/**
 * A class that represents the private part of the OPE key pair.
 * 
 */
class OPE_PrivateKey {
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
    public final Integer decrypt(Integer val) {
        return this.cipher.decrypt(val);
    }
}
