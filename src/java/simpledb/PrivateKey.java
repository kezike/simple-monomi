package simpledb;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * A class that represents the private part of the Paillier key pair.
 * 
 * Borrowed from https://github.com/kunerd/jpaillier
 */
class PrivateKey implements Serializable {

    private final BigInteger lambda;
    private final BigInteger preCalculatedDenominator;

    PrivateKey(BigInteger lambda, BigInteger preCalculatedDenominator) {
        this.lambda = lambda;

        this.preCalculatedDenominator = preCalculatedDenominator;
    }

    public BigInteger getLambda() {
        return lambda;
    }

    public BigInteger getPreCalculatedDenominator() {
        return preCalculatedDenominator;
    }
    
    public String toString() {
        return "Lambda: " + getLambda() + " PreCalculatedDenominator: " + getPreCalculatedDenominator();
    }
}