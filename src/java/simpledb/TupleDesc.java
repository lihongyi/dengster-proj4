package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> TDItems;

    public ArrayList<TDItem> getTDItems() {
        return this.TDItems;
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
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
    	int counter = 0;
        this.TDItems = new ArrayList<TDItem>();
    	for (Type a : typeAr) {
    	    String fieldName = fieldAr[counter];
            TDItem newlyCreatedTDItem = new TDItem(a, fieldName);
            TDItems.add(newlyCreatedTDItem);
            counter++;
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.TDItems = new ArrayList<TDItem>();
        for (Type a: typeAr) {
            TDItem newlyCreatedTDItem = new TDItem(a, "");
            TDItems.add(newlyCreatedTDItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.size();
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
        if (i >= TDItems.size() || i < 0) {
            throw new NoSuchElementException("No such element.");
        } else {
            return TDItems.get(i).fieldName;
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
        if (i >= TDItems.size() || i < 0) {
            throw new NoSuchElementException("No such element.");
        } else {
            return TDItems.get(i).fieldType;
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
        int counter = 0;
        for (TDItem td_item : TDItems) {
            if (td_item.fieldName.equals(name)) {
                return counter;
            }
            counter++;
        }
        throw new NoSuchElementException("No such element.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int running_count = 0;
        for (TDItem td_item : TDItems) {
            if (td_item.fieldType.equals(Type.INT_TYPE)) {
                running_count += td_item.fieldType.getLen();
            } else if (td_item.fieldType.equals(Type.STRING_TYPE)) {
                running_count += td_item.fieldType.getLen();
            }
        }
        return running_count;
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
        ArrayList<TDItem> ar1 = td1.getTDItems();
        int ar1_length = td1.numFields();
        ArrayList<TDItem> ar2 = td2.getTDItems();
        int ar2_length = td2.numFields();

        Type[] typeArray = new Type[ar1_length + ar2_length];
        String[] strArray = new String[ar1_length + ar2_length];

        int counter = 0;

        for (TDItem t1 : ar1) {
            typeArray[counter] = t1.fieldType;
            strArray[counter] = t1.fieldName;
            counter++;
        }

        for (TDItem t2: ar2) {
            typeArray[counter] = t2.fieldType;
            strArray[counter] = t2.fieldName;
            counter++;
        }

        TupleDesc retVal = new TupleDesc(typeArray, strArray);

        return retVal;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            TupleDesc incomingTupleDesc = (TupleDesc) o;
            boolean retVal = false;
            boolean same_size = false;
            boolean nth_tracker = true;
            if (incomingTupleDesc.numFields() == this.numFields()) {
                same_size = true;
                int counter = 0;
                ArrayList<TDItem> o_TDItems = incomingTupleDesc.getTDItems();
                for (TDItem a : this.TDItems) {
                    TDItem o_TDItem = o_TDItems.get(counter);
                    if (!a.fieldType.equals(o_TDItem.fieldType)) {
                        nth_tracker = false;
                    }
                    counter++;
                }
            }
            if (same_size && nth_tracker) {
                retVal = true;
            }
            return retVal;
        } else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        return "";
    }
}
