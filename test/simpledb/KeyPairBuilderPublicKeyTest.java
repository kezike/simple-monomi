package simpledb;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

public class KeyPairBuilderPublicKeyTest {

    final int BITS = 1024;

    private Paillier_KeyPairBuilder keygen;
    private Paillier_KeyPair keypair;
    private Paillier_PublicKey publicKey;

    @Before
    public void init() {
        this.keygen = new Paillier_KeyPairBuilder();
        this.keypair = keygen.generateKeyPair();
        this.publicKey = keypair.getPublicKey();
    }

    @Test
    public void testBitsSetup() {
        assertEquals(BITS, publicKey.getBits());
    }
    
    @Test
    public void testCalculationOfNSquared() {

        BigInteger n = publicKey.getN();
        BigInteger nSquared = n.multiply(n);

        assertEquals(nSquared, publicKey.getnSquared());
    }

    @Test
    public void testCalculationOfGOfG() {
        Paillier_PrivateKey privateKey = keypair.getPrivateKey();

        BigInteger n = publicKey.getN();
        BigInteger nSquared = publicKey.getnSquared();
        BigInteger g = publicKey.getG();
        BigInteger lambda = privateKey.getLambda();

        BigInteger l = g.modPow(lambda, nSquared);
        l = l.subtract(BigInteger.ONE);
        l = l.divide(n);

        assertEquals(BigInteger.ONE, l.gcd(n));
    }

}
