package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.myGbField = gbfield;
        this.myGbFieldType = gbfieldtype;
        this.myAField = afield;
        this.myWhat = what;
        this.myAggregates = new HashMap<Object, Integer>();
        this.myAverager = new HashMap<Object, Integer>();

    }

    private int myGbField;
    private Type myGbFieldType;
    private int myAField;
    private Op myWhat;
    private HashMap<Object, Integer> myAggregates;
    private HashMap<Object, Integer> myAverager;

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Object key;

        if (this.myGbField == Aggregator.NO_GROUPING)  {
            key = null; /** No one really cares what the hell this is. */
        } 

        Field field = tup.getField(this.myGbField);

        /** Need to cast field in order to get its value. It's either int or String. */
        if (field.getType() == Type.INT_TYPE) {
            IntField intfield = (IntField) field;
            key = intfield.getValue();
        } else {
            StringField stringfield = (StringField) field;
            key = stringfield.getValue();
        }

        /** We are getting the "incoming" value. */
        int incoming_value = ((IntField) tup.getField(this.myAField)).getValue();
        
        int value_so_far;
        if (!this.myAggregates.containsKey(key)) {
            int the_val;
            if (this.myWhat == Op.MIN) {
                the_val = Integer.MAX_VALUE;
            } else if (this.myWhat == Op.MAX) {
                the_val = Integer.MIN_VALUE;
            } else {
                the_val = 0;
            }
            this.myAggregates.put(key, the_val);
            this.myAverager.put(key, 0);
        }
        value_so_far = this.myAggregates.get(key);

        /** All the different cases. */
        if (this.myWhat == Op.MIN) {
            if (value_so_far > incoming_value) {
                value_so_far = incoming_value;
            }
        } else if (this.myWhat == Op.MAX) {
            if (value_so_far < incoming_value) {
                value_so_far = incoming_value;
            }
        } else if (this.myWhat == Op.AVG) {
            value_so_far = value_so_far + incoming_value;
            int how_many_keys = this.myAverager.get(key);
            how_many_keys++;
            this.myAverager.put(key, how_many_keys);
        } else if (this.myWhat == Op.COUNT) {
            value_so_far++;
        } else if (this.myWhat == Op.SUM) {
            value_so_far += incoming_value;
        }
        this.myAggregates.put(key, value_so_far);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

        /** Returned iterator. */
        ArrayList<Tuple> final_iterator = new ArrayList<Tuple>();


        TupleDesc myTupleDesc;
        if (this.myGbField != Aggregator.NO_GROUPING) {
            Type[] tArray = new Type[2];
            String[] sArray = new String[2];

            tArray[0] = this.myGbFieldType;
            tArray[1] = Type.INT_TYPE;
            sArray[0] = "key";
            sArray[1] = this.myWhat.toString();

            myTupleDesc = new TupleDesc(tArray, sArray);
        } else {
            Type[] tArray = new Type[1];
            tArray[0] = Type.INT_TYPE;
            myTupleDesc = new TupleDesc(tArray);
        }
        for (Object obj : this.myAggregates.keySet()) {
            int value_so_far = this.myAggregates.get(obj);
            Tuple tup = new Tuple(myTupleDesc);

            Field groupBy;
            if (this.myGbField == Aggregator.NO_GROUPING) {
                groupBy = new IntField(0); /** Nobody cares. */
            } else if (this.myGbFieldType == Type.STRING_TYPE) {
                groupBy = new StringField((String) obj, this.myGbFieldType.getLen());
            } else /**Then we have a string. */ {
                groupBy = new IntField((Integer) obj);
            }

            if (this.myWhat == Op.AVG) {
                value_so_far = value_so_far / this.myAverager.get(obj);
            }

            if (this.myGbField != Aggregator.NO_GROUPING) {
                tup.setField(0, groupBy);
                tup.setField(1, new IntField(value_so_far));
            } else {
                tup.setField(0, new IntField(value_so_far));
            }
            final_iterator.add(tup);
        }

        return new TupleIterator(myTupleDesc, final_iterator);

    }

}
