package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

public class JPaillierTest {

    private KeyPair keyPair;
    PublicKey publicKey;

    @Before
    public void init() {
        KeyPairBuilder keygen = new KeyPairBuilder();
        keyPair = keygen.generateKeyPair();
        publicKey = keyPair.getPublicKey();
    }

    @Test
    public void testEncryption() {
        BigInteger plainData = BigInteger.valueOf(10);
        System.out.println("Plain Data: " + plainData);

        BigInteger encryptedData = publicKey.encrypt(plainData);
        System.out.println("Encrypted Data: " + encryptedData);
        assertFalse(plainData.equals(encryptedData));
    }

    @Test
    public void testDecyption() {
        BigInteger plainData = BigInteger.valueOf(10);

        BigInteger encryptedData = publicKey.encrypt(plainData);
        BigInteger decryptedData = keyPair.decrypt(encryptedData);

        assertEquals(plainData, decryptedData);
    }
}
