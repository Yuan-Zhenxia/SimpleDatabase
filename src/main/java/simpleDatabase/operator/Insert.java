package simpleDatabase.operator;

import simpleDatabase.basic.Database;
import simpleDatabase.basic.Type;
import simpleDatabase.cache.BufferPool;
import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.field.IntField;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.tx.TransactionId;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 *
 * 从 child 操作符读取行数据 写入到特定的表中
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator childOp;

    private int count;

    private TupleDesc td;

    private boolean hasAccessed;

    private TransactionId tid;

    private int tableId;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.childOp = child;
        this.tid = t;
        this.tableId = tableId;
        count = 0;
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
        childOp.open();
        super.open();
        // insert 方法写在open函数中
        while (childOp.hasNext()) {
            Tuple next = childOp.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, next);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        super.close();
        childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * 返回插入的数据
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // 由于只会插入一次，所以可以在open函数中插入行数据，确保只会执行一次。
        // 这里虽然是插入函数，插入代码也可以写在这里，只不过判断没有插入过再插入 并且返回数据
        if (hasAccessed) return null;
        hasAccessed = true;
        Tuple insertRow = new Tuple(getTupleDesc());
        insertRow.setField(0, new IntField(count));
        return insertRow;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{childOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length == 0) throw new IllegalArgumentException("");
        childOp = children[0];
    }
}
