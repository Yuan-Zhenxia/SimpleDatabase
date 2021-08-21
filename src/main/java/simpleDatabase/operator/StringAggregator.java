package simpleDatabase.operator;

import simpleDatabase.basic.Type;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.field.Field;
import simpleDatabase.field.IntField;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.iterator.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * finished on 4 Aug. by Simin
 *
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gfield, afield;
    private TupleDesc previousTd, currentTd; // 原表中的行的描述信息，聚合的行描述信息
    private Type gbFieldType; // 分组依据的Field的类型
    private Op aop;
    private HashMap<Field, Integer> gb2val;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     *
     * 由于聚合后的行描述信息 tupleDesc需要 Aggretgate传入，所以构造参数加了个值
     *
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc tupleDesc) {
        // some code goes here
        if (aop != Op.COUNT) throw new IllegalArgumentException("String type only support COUNT operation");
        this.gfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.aop = what;
        this.currentTd = tupleDesc;
        gb2val = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field aggField = tup.getField(afield), gbField = null;
        int newVal;

        if (aggField.getType() != Type.STRING_TYPE)
            throw new IllegalArgumentException("String aggregator only accepts STRING_TYPE");

        if (previousTd == null) previousTd = tup.getTupleDesc();
        else if (!previousTd.equals(tup.getTupleDesc()))
            throw new IllegalArgumentException("tupleDesc in tables is not equals with the tuple in operation");
        if (gfield != Aggregator.NO_GROUPING) gbField = tup.getField(gfield);

        if (gb2val.containsKey(gbField)) {
            int oldV = gb2val.get(gbField);
            newVal = oldV + 1;
        } else newVal = 1;
        gb2val.put(gbField, newVal);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> item : gb2val.entrySet()) {
            Tuple t = new Tuple(currentTd);
            if (gfield == Aggregator.NO_GROUPING)
                t.setField(0, new IntField(item.getValue()));
            else {
                t.setField(0, item.getKey());
                t.setField(1, new IntField(item.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(currentTd, tuples);
    }

}
