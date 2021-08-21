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
 * finished on 7 Aug. 2021
 *
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;

    private OpIterator child;

    private TupleDesc td;

    private boolean hasAccessed;

    private int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here

        tid = t;
        this.child = child;
        count = 0;
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        child.open();
        super.open();
        hasAccessed = false;
        while (child.hasNext()) {
            Tuple next = child.next();
            Database.getBufferPool().deleteTuple(tid, next);
            ++count;
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasAccessed) return null;
        hasAccessed = true;
        Tuple deleteRow = new Tuple(getTupleDesc());
        deleteRow.setField(0, new IntField(count));
        return deleteRow;
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
