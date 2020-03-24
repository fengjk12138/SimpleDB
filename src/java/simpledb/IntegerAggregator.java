package simpledb;

import javax.print.attribute.standard.NumberUp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     * the 0-based index of the group-by field in the tuple, or
     * NO_GROUPING if there is no grouping
     * @param gbfieldtype
     * the type of the group by field (e.g., Type.INT_TYPE), or null
     * if there is no grouping
     * @param afield
     * the 0-based index of the aggregate field in the tuple
     * @param what
     * the aggregation operator
     */
    int gbField, aField;
    Type gbFieldType;
    Op operator;
    List<Tuple> tupList;
    List<Integer> corrSum, corrNum, corrAns;
    List<Tuple> ans;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        operator = what;
        gbField = gbfield;
        aField = afield;
        gbFieldType = gbfieldtype;
        tupList = new ArrayList<>();
        corrNum = new ArrayList<>();
        corrSum = new ArrayList<>();
        ans = new ArrayList<>();
        corrAns=new ArrayList<>();
    }

    public static Tuple getCorrTuple(Tuple tup, int i) {
        Type[] typeT = new Type[1];
        String[] nameT = new String[1];

        typeT[0] = tup.getTupleDesc().getFieldType(i);
        nameT[0] = tup.getTupleDesc().getFieldName(i);
        Tuple tupT = new Tuple(new TupleDesc(typeT, nameT));
        tupT.setField(0, tup.getField(i));
        return tupT;
    }

    public static Tuple getNewIntTuple(int i) {

        Type[] typeT = new Type[1];
        String[] nameT = new String[1];

        typeT[0] = Type.INT_TYPE;
        nameT[0] = "";
        Tuple ans = new Tuple(new TupleDesc(typeT, nameT));

        ans.setField(0, new IntField(i));

        return ans;
    }

    private void update(int i, int val) {
        if (corrSum.size() < corrNum.size()) {
            corrSum.add(val);
        } else if (operator == Op.MIN) {
            corrSum.set(i, Math.min(corrSum.get(i), val));
        } else if (operator == Op.MAX) {
            corrSum.set(i, Math.max(corrSum.get(i), val));
        } else if (operator == Op.SUM) {
            corrSum.set(i, corrSum.get(i) + val);
        } else if (operator == Op.AVG) {
            corrSum.set(i, corrSum.get(i) + val);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */

    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbField == NO_GROUPING) {
            if (corrNum.size() == 0)
                corrNum.add(1);
            else corrNum.set(0, corrNum.get(0) + 1);
            update(0, tup.getField(aField).hashCode());
        } else {
            int now = -1;
            for (int i = 0; i < tupList.size(); i++) {
                if (tupList.get(i).getField(0).equals(getCorrTuple(tup, gbField).getField(0))) {
                    now = i;
                    corrNum.set(i, corrNum.get(i) + 1);
                    break;
                }
            }
            if (now == -1) {
                corrNum.add(1);
                now = corrNum.size() - 1;
                tupList.add(getCorrTuple(tup, gbField));
            }
            update(now, tup.getField(aField).hashCode());
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//                UnsupportedOperationException("please implement me for lab2");
        corrAns.clear();
        for (int i = 0; i < corrNum.size(); i++) {
            if (operator == Op.AVG) {
                corrAns.add(i, corrSum.get(i) / corrNum.get(i));
            } else if (operator == Op.COUNT) {
                corrAns.add(i, corrNum.get(i));
            } else {
                corrAns.add(i, corrSum.get(i));
            }
        }
        if (gbField == NO_GROUPING) {
            ans.add(getNewIntTuple(corrAns.get(0)));


        } else {
            for (int i = 0; i < corrNum.size(); i++) {
                ans.add(Join.mergeTuple(tupList.get(i),
                        getNewIntTuple(corrAns.get(i))));

            }
        }
//        System.out.println(String.format("---%d %d", corrNum.size(), tupList.size()));
        return new TupleArrayIterator((ArrayList<Tuple>) ans);


    }

}
