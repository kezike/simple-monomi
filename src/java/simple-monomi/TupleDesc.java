package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TDItem> {

    private ArrayList<TDItem> items;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    @Override
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Constructor: Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr.length == fieldAr.length;
        this.items = new ArrayList<TDItem>();
        int numFlds = fieldAr.length;
        for (int i = 0; i < numFlds; i++) {
          if (fieldAr[i] == null) {
            fieldAr[i] = "null";
          }
          TDItem newItem = new TDItem(typeAr[i], fieldAr[i]);
          this.items.add(newItem);
        }
    }

    /**
     * Constructor: Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.items = new ArrayList<TDItem>();
        int numFlds = typeAr.length;
        for (int i = 0; i < numFlds; i++) {
          TDItem newItem = new TDItem(typeAr[i], "null");
          this.items.add(newItem);
        }
    }

    /**
     * Constructor: Create a new tuple desc with provided items
     * 
     * @param items
     *            list specifying the ordered set of field descriptions
     *            which includes associated field names and types.
     */
    public TupleDesc(ArrayList<TDItem> items) {
        this.items = items;
    }

    /**
     * Gets the underlying items of this instance of TupleDesc.
     * 
     * @return list of items associated to this TupleDesc instance
     */
    public ArrayList<TDItem> getItems() {
        return new ArrayList<TDItem>(this.items);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
          return this.items.get(i).getFieldName();
        } catch (IndexOutOfBoundsException iobExn) {
          String errMsg = String.format("No element resides at index %d", i);
          throw new NoSuchElementException(errMsg);
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
          return this.items.get(i).getFieldType();
        } catch (IndexOutOfBoundsException iobExn) {
          String errMsg = String.format("No element resides at index %d of this tuple schema", i);
          throw new NoSuchElementException(errMsg);
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
          String errMsg = String.format("No field with null name value resides in this tuple schema");
          throw new NoSuchElementException(errMsg);
        }
        int numFlds = this.numFields();
        for (int i = 0; i < numFlds; i++) {
          if (this.getFieldName(i) == null) {
            continue;
          }
          if (this.getFieldName(i).equals(name)) {
            return i;
          }
        }
        String errMsg = String.format("No element named '" + name + "' resides in this tuple schema");
        throw new NoSuchElementException(errMsg);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sizeBytes = 0;
        for (TDItem tdItem : this.items) {
          sizeBytes += tdItem.getFieldType().getLen();
        }
        return sizeBytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<TDItem> items = new ArrayList<TDItem>();
        for (TDItem tdItem1 : td1) {
          items.add(tdItem1);
        }
        for (TDItem tdItem2 : td2) {
          items.add(tdItem2);
        }
        return new TupleDesc(new ArrayList<TDItem>(items));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) {
          return false;
        }
        TupleDesc thatTD = (TupleDesc) o;
        int thisNumFields = this.numFields();
        int thatNumFields  = thatTD.numFields();
        if (thisNumFields != thatNumFields) {
          return false;
        }
        for (int i = 0; i < thisNumFields; i++) {
          if (this.getFieldType(i) != thatTD.getFieldType(i)) {
            return false;
          }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("Unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder tdStrBldr = new StringBuilder();
        int numFlds = this.numFields();
        for (int i = 0; i < numFlds; i++) {
          tdStrBldr.append(this.items.get(i).toString());
          if (i == numFlds - 1) {
            break;
          }
          tdStrBldr.append(", ");
        }
        return tdStrBldr.toString();
    }
}
