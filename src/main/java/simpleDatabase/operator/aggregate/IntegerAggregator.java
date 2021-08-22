package simpleDatabase.operator.aggregate;

import simpleDatabase.basic.Type;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.field.Field;
import simpleDatabase.field.IntField;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.iterator.TupleIterator;
import simpleDatabase.operator.aggregate.Aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * finished on 4 Aug. 2021
 *
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int afield, gfield;
    TupleDesc previousTd; // 原数据的行描述
    TupleDesc currentTd; // 聚合后的行描述
    Type gType;
    Op aop;
    // group by之后的结果，group by的Field作为key，结果是val。聚合后的值求平均数可能是double类型
    HashMap<Field, Integer> gb2val;
    // 例如count函数记录了多少个 值，
    HashMap<Field, Integer[]> gb2sum;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     *            使用哪一列来分组，group by
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc tupleDesc) {
        this.gfield = gbfield;
        this.gType = gbfieldtype;
        this.afield = afield;
        this.aop = what;
        this.currentTd = tupleDesc;
        gb2sum = new HashMap<>();
        gb2val = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        // some code goes here
        Field aggField = tup.getField(afield), gbField = null;
        if (aggField.getType() != Type.INT_TYPE)
            throw new IllegalArgumentException("INTEGER_AGGREGATOR not accept INT_TYPE");

        int toAggregate = ((IntField) aggField).getValue(), newVal = 0;
        if (previousTd == null) previousTd = tup.getTupleDesc();
        else if (!previousTd.equals(tup.getTupleDesc()))
            throw new IllegalArgumentException("Tuple type is not equals with tupleDesc");

        if (gfield != Aggregator.NO_GROUPING)
            gbField = tup.getField(gfield);

        if (aop == Op.AVG) {
            if (gb2sum.containsKey(gbField)) {
                Integer[] v = gb2sum.get(gbField);
                gb2sum.put(gbField, new Integer[]{v[0] + 1, v[1] + toAggregate});
            } else
                gb2sum.put(gbField, new Integer[]{1, toAggregate});
            Integer[] v = gb2sum.get(gbField);
            gb2val.put(gbField, (v[1] / v[0]));
            return;
        }

        if (gb2val.containsKey(gbField)) {
            int oldVal = gb2val.get(gbField);
            newVal = calVal(oldVal, toAggregate, aop);
        } else if (aop == Op.COUNT) {
            newVal = 1; // 如果是计数，第一次设置1
        } else
            newVal = toAggregate; // 其他操作，都需要设置成目标值

        gb2val.put(gbField, newVal);
    }

    public int calVal(int oldV, int toAgg, Op op) {
        switch (aop) {
            case COUNT:
                return oldV + 1;
            case MAX:
                return oldV > toAgg ? oldV : toAgg;
            case MIN:
                return oldV > toAgg ? toAgg : oldV;
            case SUM:
                return oldV + toAgg;
            default:
                throw new IllegalArgumentException("unknown Exception");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gb2val.entrySet()) {
            // 这是聚合的tuple 不是原表中的tuple，该tuple最多含两个Field
            Tuple t = new Tuple(currentTd);
            if (gfield == Aggregator.NO_GROUPING)
                t.setField(0, new IntField(g2a.getValue()));
            else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(currentTd, tuples);
    }

}
