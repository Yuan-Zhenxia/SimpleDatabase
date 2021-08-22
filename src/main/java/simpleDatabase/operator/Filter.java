package simpleDatabase.operator;

import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.iterator.OpIterator;
import simpleDatabase.iterator.TupleIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 *
 * finished on 8 Aug. 2021
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    private TupleDesc td;

    private OpIterator child;

    /* 缓存过滤结果 */
    private TupleIterator filterCache;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.child = child;
        this.predicate = p;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, IOException {
        // some code goes here
        child.open();
        super.open();
        filterCache = filter(child, predicate);
        filterCache.open();
    }

    /**
     * 调用Predicate中的filter方法，来实现过滤方法
     * 过滤器中只需要写好过程。具体的filter函数由Predicate来实现
     * @param child
     * @param predicate
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private TupleIterator filter (OpIterator child, Predicate predicate) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (predicate.filter(t)) {
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        filterCache = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        filterCache.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (filterCache.hasNext()) return filterCache.next();
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
