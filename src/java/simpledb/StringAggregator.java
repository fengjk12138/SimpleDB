package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    int gbField, aField;
    Type gbFieldType;
    List<Integer> numCount;
    List<Tuple> gbTuple;
    List<Tuple> ans;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        gbFieldType = gbfieldtype;
        gbField = gbfield;
        aField = afield;
        gbTuple = new ArrayList<>();
        numCount = new ArrayList<>();
        ans = new ArrayList<>();
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (gbField == NO_GROUPING) {
            if (numCount.size() == 0)
                numCount.add(0);
            numCount.set(0, numCount.get(0) + 1);
        } else {
            int now = -1;
            for (int i = 0; i < gbTuple.size(); i++) {
                if (gbTuple.get(i).getField(0).equals(
                        IntegerAggregator.getCorrTuple(tup, gbField).getField(0))) {
                    now = i;
                    break;
                }
            }
            if (now == -1) {
                gbTuple.add(IntegerAggregator.getCorrTuple(tup, gbField));
                now = gbTuple.size() - 1;
                numCount.add(0);

            }
            numCount.set(now, numCount.get(now) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (numCount.size() == 0) {
            return null;
        }
        if (gbField == NO_GROUPING) {
            ans.add(IntegerAggregator.getNewIntTuple(numCount.get(0)));
        } else {
            for (int i = 0; i < numCount.size(); i++) {
                Tuple tmpT = IntegerAggregator.getNewIntTuple(numCount.get(i));
                ans.add(Join.mergeTuple(gbTuple.get(i), tmpT));
            }
        }
        return new TupleArrayIterator((ArrayList<Tuple>) ans);

//        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
