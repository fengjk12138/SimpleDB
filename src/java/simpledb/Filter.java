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
     * The predicate to filter tuples with
     * @param child
     * The child operator
     */
    Predicate predicate;
    OpIterator childOp;

    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        predicate = p;
        childOp = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return childOp.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        ;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOp.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple now = null;
        childOp.open();
        while (childOp.hasNext()) {
            Tuple tmp = childOp.next();
            if (predicate.filter(tmp)) {
                now = tmp;
                break;
            }
        }
        return now;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] it=new OpIterator[1];
        it[0]=childOp;
        return it;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        childOp=children[0];
    }

}
