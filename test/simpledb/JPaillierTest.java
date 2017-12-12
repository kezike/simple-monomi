package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;
import java.security.SecureRandom;

import org.junit.Before;
import org.junit.Test;

public class JPaillierTest {

    private Paillier_KeyPair keyPair;
    Paillier_PublicKey publicKey;

    @Before
    public void init() {
        Paillier_KeyPairBuilder keygen = new Paillier_KeyPairBuilder();
        keyPair = keygen.generateKeyPair();
        publicKey = keyPair.getPublicKey();
    }

    @Test
    public void testEncryption() {
        SecureRandom rng = new SecureRandom();
        BigInteger plainData = BigInteger.probablePrime(15, rng);
        System.out.println("Plain Data: " + plainData);

        BigInteger encryptedData = publicKey.encrypt(plainData);
        System.out.println("Encrypted Data: " + encryptedData);
        assertFalse(plainData.equals(encryptedData));
    }

    @Test
    public void testDecyption() {
        SecureRandom rng = new SecureRandom();
        BigInteger plainData = BigInteger.probablePrime(15, rng);

        BigInteger encryptedData = publicKey.encrypt(plainData);
        BigInteger decryptedData = keyPair.decrypt(encryptedData);

        assertEquals(plainData, decryptedData);
    }
}
