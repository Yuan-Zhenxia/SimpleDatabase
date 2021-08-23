package simpleDatabase.operator.join;

import simpleDatabase.basic.Type;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.iterator.TupleIterator;
import simpleDatabase.operator.Operator;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 *
 * finished
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;

    private TupleDesc td;

    /* 需要join的两个 */
    private OpIterator child1, child2;

    /* join后的tuples, 需要返回的结果 */
    private TupleIterator joinTuples;

    /**
     * Nested Loop Join
     * NLJ算法：将外层表的结果集作为循环的基础数据，
     * 然后循环从该结果集每次一条获取数据作为下一个表的过滤条件去查询数据，然后合并结果。
     * 如果有多个表join，那么应该将前面的表的结果集作为循环数据，
     * 取结果集中的每一行再到下一个表中继续进行循环匹配，获取结果集并返回给客户端。
     *
     * Block Nested-Loop Join算法
     *
     * BNL算法原理：将外层循环的行/结果集存入join buffer，
     * 内存循环的每一行数据与整个buffer中的记录做比较，可以减少内层循环的扫描次数
     *
     * 举个简单的例子：外层循环结果集有1000行数据，使用NLJ算法需要扫描内层表1000次，
     * 但如果使用BNL算法，则先取出外层表结果集的100行存放到join buffer,
     * 然后用内层表的每一行数据去和这100行结果集做比较，可以一次性与100行数据进行比较，
     * 这样内层表其实只需要循环1000/100=10次，减少了9/10。
     *
     * @link https://blog.csdn.net/qq_34291570/article/details/92974736
     *
     * MySQL 使用bnl算法的默认缓冲区大小 字节单位
     */
    public static final int blockNestedJoinBufferSize = 131072;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.joinPredicate = p;
        /* 获取待join的两个子集合的field数量 */
        int len1 = child1.getTupleDesc().numFields();
        int len2 = child2.getTupleDesc().numFields();
        Type[] types = new Type[len1 + len2];
        String[] names = new String[types.length];
        td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     *      * Block Nested-Loop Join算法
     *      *
     *      * BNL算法原理：将外层循环的行/结果集存入join buffer，
     *      * 内存循环的每一行数据与整个buffer中的记录做比较，可以减少内层循环的扫描次数
     *      *
     *      * 举个简单的例子：外层循环结果集有1000行数据，使用NLJ算法需要扫描内层表1000次，
     *      * 但如果使用BNL算法，则先取出外层表结果集的100行存放到join buffer,
     *      * 然后用内层表的每一行数据去和这100行结果集做比较，可以一次性与100行数据进行比较，
     *      * 这样内层表其实只需要循环1000/100=10次，减少了9/10。
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private TupleIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tupleLinkedList = new LinkedList<>();
        /* 获取child1的tuple的总共长度，blockNum表明该缓冲池能存放的最多block数量 */
        int blockNum = blockNestedJoinBufferSize / child1.getTupleDesc().getSize();
        /* 缓存的tuple数组 */
        Tuple[] cachedTuples = new Tuple[blockNum];
        int idx = 0; /* 计数器，记录缓存中的数字 */
        int len1 = child1.getTupleDesc().numFields();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple next = child1.next();
            cachedTuples[idx++] = next;
            /* 如果缓冲区满了, 就先把缓冲区中的tuples和第二张表中的数据都处理一下 */
            if (idx >= cachedTuples.length) {
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    for (Tuple cachedLeft : cachedTuples) {
                        /* 把符合要求的左右数据join起来 */
                        if (joinPredicate.filter(cachedLeft, right)) {
                            Tuple re = mergeTuples(len1, cachedLeft, right);
                            tupleLinkedList.add(re);
                        }
                    }
                }
                Arrays.fill(cachedTuples, null);
                idx = 0;
            }
        }
        /* 处理剩下的tuples */
        if (idx > 0 && idx < cachedTuples.length) {
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                for (Tuple cacheLeft : cachedTuples) {
                    if (cacheLeft == null) break; /* 最后一次cache中可能并没有满，会有空值 */
                    if (joinPredicate.filter(cacheLeft, right)) {
                        Tuple result = mergeTuples(len1, cacheLeft, right);
                        tupleLinkedList.add(result);
                    }
                }
            }
        }
        return new TupleIterator(getTupleDesc(), tupleLinkedList);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
        joinTuples = blockNestedLoopJoin();
        joinTuples.open();
    }

    /* 第一个传入的是左边的参数长度 */
    private Tuple mergeTuples(int len, Tuple left, Tuple right) {
        Tuple result = new Tuple(td);
        /* 设置左边的tuple属性 */
        for (int i = 0; i < len; i++)
            result.setField(i, left.getField(i));
        /* 设置右边的tuple属性 */
        for (int i = 0; i < child2.getTupleDesc().numFields(); i++)
            result.setField(i + len, right.getField(i));

        return result;
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        joinTuples.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        joinTuples.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (joinTuples.hasNext()) return joinTuples.next();
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
