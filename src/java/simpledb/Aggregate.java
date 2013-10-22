package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private boolean grouping;
    private TupleDesc td;
    private TupleDesc newTD;
    private Aggregator agg;
    private DbIterator agg_iter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	   this.child = child;
       this.td = child.getTupleDesc();
       this.afield = afield;
       this.gfield = gfield;
       this.aop = aop;
       this.grouping = (gfield != Aggregator.NO_GROUPING);
       
       Type[] typeAr = new Type[2];
       String[] fieldAr = new String[2];
       Type afieldType = this.td.getFieldType(afield);

       if (this.grouping) {
           Type gfieldType = this.td.getFieldType(gfield);
            typeAr = new Type[]{ gfieldType , afieldType };
            fieldAr = new String[]{ aop.toString() + gfieldType, this.td.getFieldName(this.afield) };
       } else {
            typeAr = new Type[]{ afieldType };
            fieldAr = new String[]{ this.td.getFieldName(this.afield) };
       }
       
       if (this.td.getFieldType(afield) == Type.INT_TYPE) {
            this.agg = new IntegerAggregator(gfield, gfieldType, afield, aop);
       } else if (this.td.getFieldType(afield) == Type.STRING_TYPE) {
            this.agg = new StringAggregator(gfield, gfieldType, afield, aop);
       }


       this.newTD = new TupleDesc(typeAr, fieldAr);


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (this.grouping) {
            return this.td.getFieldName(this.gfield);
        }
	      
        return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	   return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	   return this.td.getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	   return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
	    this.child.open();
        while(this.child.hasNext()) {
            this.agg.mergeTupleIntoGroup(this.child.next());
        }
        this.agg_iter = this.agg.iterator();
        this.agg_iter.open();
        super.open();
    }


    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if (this.agg_iter.hasNext()) {
            return this.agg_iter.next();
        } else {
	       return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
	   this.agg_iter = this.agg.iterator();
       this.agg_iter.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	   return this.newTD;
    }

    public void close() {
	   super.close();
       this.agg_iter = null;
       this.child.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	   this.child = children[0];
    }
    
}
