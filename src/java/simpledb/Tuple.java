package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupDesc;
    private RecordId recId;
    private ArrayList<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupDesc = td;
        this.fields = new ArrayList<Field>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if (this.tupDesc == null) {
    		System.out.println("EMPTY TUPDESC");
    	}
        int numFlds = this.tupDesc.numFields();
        if (i < 0 || i >= numFlds) {
          return;
        }
        int numFldsCum = this.fields.size();
        if (i < numFldsCum) {
          this.fields.set(i, f);
          return;
        }
        int fldIter = i;
        while (fldIter - (numFldsCum - 1) > 0) {
          this.fields.add(null);
          numFldsCum++;
        }
        this.fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        int numFlds = this.tupDesc.numFields();
        if (i < 0 || i >= numFlds) {
          return null;
        }
        int numFldsCum = this.fields.size();
        if (i >= numFldsCum) {
          return null;
        } else if (this.fields.get(i) == null) {
          return null;
        }
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder tdStrBldr = new StringBuilder();
        int numFlds = this.tupDesc.numFields();
        for (int i = 0; i < numFlds; i++) {
          tdStrBldr.append(this.getField(i).toString());
          if (i == numFlds - 1) {
            break;
          }
          tdStrBldr.append("\t");
        }
        return tdStrBldr.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tupDesc = td;
        this.fields = new ArrayList<Field>();
    }
}
