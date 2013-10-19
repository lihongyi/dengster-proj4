package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.myField1 = field1;
        this.myOp = op;
        this.myField2 = field2;
    }

    private int myField1;
    private Predicate.Op myOp;
    private int myField2;

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Field f1 = t1.getField(this.myField1);
        Field f2 = t2.getField(this.myField2);
        boolean retVal = f1.compare(this.myOp, f2);
        return retVal;
    }
    
    public int getField1()
    {
        // some code goes here
        return this.myField1;
    }
    
    public int getField2()
    {
        // some code goes here
        return this.myField2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return this.myOp;
    }
}
