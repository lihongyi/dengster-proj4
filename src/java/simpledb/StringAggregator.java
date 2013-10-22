package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private TupleDesc tupleDesc;
    private boolean grouping;
    private Hashtable<Field,Integer> countTable;
    private int count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.grouping = (gbfield != Aggregator.NO_GROUPING);
        
        Type[] typeAr = new Type[2];
        String[] fieldAr = new String[2];

        if (this.grouping) {
            typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            fieldAr = new String[]{what.toString(), "count"}; 
            this.countTable = new Hashtable<Field, Integer>();
        } else {
            typeAr = new Type[]{Type.INT_TYPE};
            fieldAr = new String[]{"count"};
            this.count = 0;
        }

        this.tupleDesc = new TupleDesc(typeAr, fieldAr);

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupField = tup.getField(this.gbField);
        
        if (this.grouping) {
            if (countTable.containsKey(tupField)) {
                int count = countTable.get(tupField);
                count ++;
                countTable.put(tupField, count);
            } else {
                countTable.put(tupField, 1);
            }
        } else {
            this.count++;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tupList = new ArrayList<Tuple>();
        Enumeration keys = this.countTable.keys();
        if (this.grouping) {
            while(keys.hasMoreElements()) {
                Field f = (Field) keys.nextElement();
                int val = this.countTable.get(f);
                Tuple newTuple = new Tuple(this.tupleDesc);
                newTuple.setField(0, f);
                IntField intF = new IntField(val);
                newTuple.setField(1, val);
                tupList.add(newTuple);
            }
        } else {
            Tuple newTuple = new Tuple(this.tupleDesc);
            IntField intF = new IntField(this.count);
            newTuple.setField(0, intF);
            tupList.add(newTuple);
        }

        return tupList.iterator();
        throw new UnsupportedOperationException("please implement me for proj2");
    }

}
