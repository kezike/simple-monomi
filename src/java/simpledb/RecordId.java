package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pgId;
    private int tplNum;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pgId = pid;
        this.tplNum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tplNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pgId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof RecordId)) {
          return false;
        }
        RecordId thatRid = (RecordId) o;
        boolean tplNumEquality = this.tplNum == thatRid.tplNum;
        boolean pgIdEquality = this.pgId.equals(thatRid.pgId);
        return tplNumEquality && pgIdEquality;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        int pgIdHashCode = this.pgId.hashCode();
        String tplNumStr = String.valueOf(this.tplNum);
        String pgIdStr = String.valueOf(pgIdHashCode);
        String tplNumPgIdStr = tplNumStr.concat(pgIdStr);
        return tplNumPgIdStr.hashCode();

    }

}
