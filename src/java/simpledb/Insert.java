package simpledb;
import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableid = tableid;

        Type[] tArray = new Type[1];
        tArray[0] = Type.INT_TYPE;

        String[] sArray = new String[1];
        sArray[0] = "Inserted shit";

        tupleDesc = new TupleDesc(tArray, sArray);
    }
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private TupleDesc tupleDesc;


    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.fetchNext) {
            int counter = 0;
            while(this.child.hasNext()) {
                Tuple tup = child.next();
                try { Database.getBufferPool().insertTuple(this.t, this.tableid, tup); }
                catch(IOException e) {

                }
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
