package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class KeyPairIntegerTest {

    final int BITS = 1024;
    final int BITS_INTEGER = 15;

    private KeyPairBuilder keygen;
    private KeyPair keypair;
    private PublicKey publicKey;

    @Before
    public void init() {
        this.keygen = new KeyPairBuilder();
        keygen.upperBound(BigInteger.valueOf(Integer.MAX_VALUE));
        keygen.bits(BITS_INTEGER);
        this.keypair = keygen.generateKeyPair();
        this.publicKey = keypair.getPublicKey();
    }

    @Test
    public void testIntegerSetup() {
        assertEquals(BITS_INTEGER, publicKey.getBits());
    } 
    
    @Test
    public void testCalculationOfNSquared() {

        BigInteger n = publicKey.getN();
        BigInteger nSquared = n.multiply(n);

        assertEquals(nSquared, publicKey.getnSquared());
    }

    @Test
    public void testCalculationOfGOfG() {
        PrivateKey privateKey = keypair.getPrivateKey();

        BigInteger n = publicKey.getN();
        BigInteger nSquared = publicKey.getnSquared();
        BigInteger g = publicKey.getG();
        BigInteger lambda = privateKey.getLambda();

        BigInteger l = g.modPow(lambda, nSquared);
        l = l.subtract(BigInteger.ONE);
        l = l.divide(n);

        assertEquals(BigInteger.ONE, l.gcd(n));
    }
    
    @Test
    public void testBigIntegerToIntegerOverFlow() {
    		
    		int i = 0; 
    		while (i < 10000) { // Test 10,000 times if the the key works
        		Random r = new Random();
        		BigInteger plainData = BigInteger.valueOf(r.nextInt());
        		System.out.println("Plain Data: " + plainData);
        		BigInteger encryptedData = publicKey.encrypt(plainData);
        		System.out.println("Encrypted Data: " + encryptedData);
        		assertFalse(plainData.equals(encryptedData));
                
        		try {
        			encryptedData.intValueExact();
        		} catch (ArithmeticException e) {
        			System.out.println("Iteration: " + i);
        			fail("Encrypted Data is too large for an integer.");
        		}	
    			i++;
    		}
    }
}
