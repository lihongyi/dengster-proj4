package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.myTupleDesc = td;
        this.myFields = new Field[td.numFields()];
    }

    private TupleDesc myTupleDesc;
    private Field[] myFields;
    private RecordId myRecordId;


    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return myTupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.myRecordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.myRecordId = rid;
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
        if (i < myFields.length && i >= 0) {
            if (myTupleDesc.getFieldType(i) != f.getType()) {
                throw new RuntimeException("Derp");
            } else {
                myFields[i] = f;
            }
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < myFields.length && i >= 0) {
            Field retVal = myFields[i];
            if (retVal != null) {
                return retVal;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        String retVal = "";
        for (int i=0; i< myFields.length; i++) {
            if (i == myFields.length-1) {
                retVal = retVal + myFields[i] + "\n";
            } else {
                retVal = retVal + myFields[i] + "\t";
            }
        }
        return retVal;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        List<Field> arrAsList = Arrays.asList(myFields);
        return arrAsList.iterator();
    }
}
