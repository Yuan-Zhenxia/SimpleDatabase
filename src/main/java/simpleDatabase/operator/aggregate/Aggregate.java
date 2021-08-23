package simpleDatabase.operator.aggregate;

import simpleDatabase.basic.Type;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.operator.Operator;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * finished on 4 Aug. 2021 by Simin Wang
 *
 * sum avg max min都是聚合操作
 *
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield, gfield;
    private TupleDesc childTd;
    private Aggregator.Op aop;
    private TupleDesc td;
    private OpIterator aggregateIt; // 聚合的结果通过这访问
    private Type gFieldType;
    private Aggregator aggregator; // 聚合操作通过这个实现

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            某一列需要聚合的下标
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        childTd = child.getTupleDesc();
        Type childType = childTd.getFieldType(afield);
        gFieldType = childTd.getFieldType(gfield);
        if (childType == Type.INT_TYPE)
            aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop, getTupleDesc());
        else if (childType == Type.STRING_TYPE)
            aggregator = new StringAggregator(gfield, gFieldType, afield, aop, getTupleDesc());
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // 如果不需要group by直接返回null
        //
        if (gfield == Aggregator.NO_GROUPING) return null;
        return aggregateIt.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        // 0 返回 NoSuchElementException
        if (gfield == Aggregator.NO_GROUPING) return aggregateIt.getTupleDesc().getFieldName(0);
        return aggregateIt.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        while (child.hasNext()) aggregator.mergeTupleIntoGroup(child.next());
        aggregateIt = aggregator.iterator();
        aggregateIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     * 如果是group by，那么第一个field就是聚合的，第二个是计算聚合的结果
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggregateIt.hasNext()) return aggregateIt.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        aggregateIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * TupleDesc中保存的事 fields数组，如果没有使用group by那么只有一个需要聚合的column，
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (td != null) return td;
        Type[] types;
        String[] names;
        String aName = childTd.getFieldName(afield); // 需要聚合的列名字
        if (gfield == Aggregator.NO_GROUPING) {
            // 如果不需要聚合 只有一列
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{aName};
        } else {
            types = new Type[]{gFieldType, Type.INT_TYPE};
            names = new String[]{childTd.getFieldName(gfield), aName};
        }
        td = new TupleDesc(types, names);
        return td;
    }

    public void close() {
        // some code goes here
        super.close();
        aggregateIt.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
