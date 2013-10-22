package simpledb;
import java.io.*;
/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        Type[] tArray = new Type[1];
        tArray[0] = Type.INT_TYPE;

        String[] sArray = new String[1];
        sArray[0] = "Deleted shit";
        tupleDesc = new TupleDesc(tArray, sArray);
    }

    private TransactionId t;
    private DbIterator child;
    private TupleDesc tupleDesc;

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.fetchNext = true;
    }

    private boolean fetchNext;

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.fetchNext) {
            int counter = 0;
            while(this.child.hasNext()) {
                Tuple tup = child.next();
                Database.getBufferPool().deleteTuple(this.t, tup);
                counter++;
            }
            Field f = new IntField(counter);
            Tuple retVal = new Tuple(this.tupleDesc);
            retVal.setField(0, f);
            this.fetchNext = false;
            return retVal;
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] retVal = new DbIterator[1];
        retVal[0] = this.child;
        return retVal;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
