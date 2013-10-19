package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.myPredicate = p;
        this.myChild = child;
        this.myChildren = new DbIterator[1];
        this.myChildren[0] = this.myChild;
    }

    private Predicate myPredicate;
    private DbIterator myChild;
    private DbIterator[] myChildren;

    public Predicate getPredicate() {
        // some code goes here
        return this.myPredicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.myChild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.myChild.open();
    }

    public void close() {
        // some code goes here
        this.myChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.myChild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (this.myChild.hasNext()) {
            Tuple retVal = this.myChild.next();
            if (this.myPredicate.filter(retVal)) { 
                return retVal;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return this.myChildren;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.myChild = children[0];
    }

}
