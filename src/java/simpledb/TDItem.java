package simpledb;

import java.io.Serializable;

/** 
 * A help class to facilitate organizing the information of each field
 * */
public class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 
     * The type of the field
     * */
    public final Type fieldType;

    /** 
     * The name of the field
     * */
    public final String fieldName;

    public TDItem(Type t, String n) {
        this.fieldType = t;
        this.fieldName = n;
    }

    public Type getFieldType() {
        return this.fieldType;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public String toString() {
        return fieldName + "(" + fieldType + ")";
    }
}
