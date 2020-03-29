package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    int tabelId;
    OpIterator operator;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        operator = child;
        this.tabelId = tableId;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert");
        }
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
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    int callednum = 0;

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (callednum != 0)
            return null;
        callednum++;
        int num = 0;
        try {


            while (operator.hasNext()) {
                num++;
                Database.getBufferPool().insertTuple(null, tabelId, operator.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return IntegerAggregator.getNewIntTuple(num);
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] tmp = new OpIterator[1];
        tmp[0] = operator;
        return tmp;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        operator = children[0];
    }
}
