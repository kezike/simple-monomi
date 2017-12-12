package simpledb;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

public class KeyPairBuilderPrivateKeyTest {

    private Paillier_KeyPairBuilder keygen;
    private Paillier_KeyPair keypair;
    private Paillier_PrivateKey privateKey;

    @Before
    public void init() {
        this.keygen = new Paillier_KeyPairBuilder();
        this.keypair = keygen.generateKeyPair();
        this.privateKey = keypair.getPrivateKey();
    }

    @Test
    public void testPreCalculatedDenominator() {
        Paillier_PublicKey publicKey = keypair.getPublicKey();

        BigInteger preCalculatedDenominator = privateKey.getPreCalculatedDenominator();

        BigInteger g = publicKey.getG();
        BigInteger n = publicKey.getN();
        BigInteger nSquared = publicKey.getnSquared();
        BigInteger lambda = privateKey.getLambda();

        BigInteger expected = g.modPow(lambda, nSquared);
        expected = expected.subtract(BigInteger.ONE);
        expected = expected.divide(n);
        expected = expected.modInverse(n);

        assertEquals(expected, preCalculatedDenominator);
    }

}
