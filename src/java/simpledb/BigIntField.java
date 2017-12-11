package simpledb;

import java.io.*;
import java.math.BigInteger;

/**
 * Instance of Field that stores a single BigInteger.
 */
public class BigIntField implements Field {
    
	private static final long serialVersionUID = 1L;
	
	private final BigInteger value;

    public BigInteger getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public BigIntField(BigInteger i) {
        value = i;
    }

    public String toString() {
        return value.toString();
    }

    public int hashCode() {
        return value.intValueExact();
    }

    public boolean equals(Object field) {
        return ((BigIntField) field).value == value;
    }

    /**
     * Write this BigInt to dos. First writes the number of bytes need to represent this
     * BigInt.
     */
    public void serialize(DataOutputStream dos) throws IOException {
//        dos.writeObject(value);
        BigInteger b = value;
        dos.writeInt(b.bitLength());
        dos.write(value.toByteArray());
        
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        BigIntField iVal = (BigIntField) val;

        switch (op) {
        case EQUALS:
            return value == iVal.value;
        case NOT_EQUALS:
            return value != iVal.value;

        case GREATER_THAN:
            return value.compareTo(iVal.value) > 0;

        case GREATER_THAN_OR_EQ:
            return value.compareTo(iVal.value) >= 0;

        case LESS_THAN:
            return value.compareTo(iVal.value) < 0;

        case LESS_THAN_OR_EQ:
            return value.compareTo(iVal.value) <= 0;

    case LIKE:
        return value == iVal.value;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.BIGINT_TYPE;
	}
}
