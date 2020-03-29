package simpledb;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

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
     * The transaction this delete runs in
     * @param child
     * The child operator from which to read tuples for deletion
     */
    OpIterator operator;
    int calledNum = 0;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        operator = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return IntegerAggregator.getNewIntTuple(0).getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        operator.open();
    }

    public void close() {
        // some code goes here
        super.close();
        operator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        operator.rewind();
        calledNum=0;
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
        if (calledNum != 0)
            return null;
        calledNum++;
        int num=0;
        while (operator.hasNext())
        {
            Tuple u=operator.next();

            try {
                Database.getBufferPool().deleteTuple(null,u);
                num++;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        return IntegerAggregator.getNewIntTuple(num);
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
