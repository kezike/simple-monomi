package simpledb;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * A class that holds a pair of associated public and private keys.
 * 
 * Borrowed from https://github.com/kunerd/jpaillier
 */
public class OPE_KeyPair implements KeyPair, Serializable {

    private final OPE_PrivateKey privateKey;
    private final OPE_PublicKey publicKey;

    OPE_KeyPair(OPE_PrivateKey privateKey, OPE_PublicKey publicKey) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
    }

    public OPE_PrivateKey getPrivateKey() {
        return this.privateKey;
    }

    public OPE_PublicKey getPublicKey() {
        return this.publicKey;
    }

    /**
     * Decrypts the given ciphertext.
     *
     * @param val The ciphertext that should be decrypted.
     * @return The corresponding plaintext.
     */
    public BigInteger decrypt(BigInteger val) {
        return this.privateKey.decrypt(val);
    }
}
