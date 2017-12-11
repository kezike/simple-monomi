package simpledb;

import java.math.BigInteger;

/**
 * A class that holds a pair of associated public and private keys.
 * 
 * Borrowed from https://github.com/kunerd/jpaillier
 */
public class OPE_KeyPair {

    private final OPE_PrivateKey privateKey;
    private final OPE_PublicKey publicKey;
    private final OPE ope;

    OPE_KeyPair(OPE_PrivateKey privateKey, OPE_PublicKey publicKey, OPE ope) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.ope = ope;
    }

    public OPE_PrivateKey getPrivateKey() {
        return this.privateKey;
    }

    public OPE_PublicKey getPublicKey() {
        return this.publicKey;
    }

    public OPE getOPE() {
        return this.ope;
    }

    /**
     * Decrypts the given ciphertext.
     *
     * @param val The ciphertext that should be decrypted.
     * @return The corresponding plaintext.
     */
    public Integer decrypt(Integer val) {
        return this.ope.decrypt(val);
    }
}