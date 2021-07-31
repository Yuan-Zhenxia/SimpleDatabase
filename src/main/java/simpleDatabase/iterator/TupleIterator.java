package simpleDatabase.iterator;

import simpleDatabase.cache.Tuple;
import simpleDatabase.cache.TupleDesc;

import java.util.Iterator;

/**
 * Implements a OpIterator by wrapping an Iterable<Tuple>.
 */
public class TupleIterator implements OpIterator {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    Iterator<Tuple> i = null;
    TupleDesc td = null;
    Iterable<Tuple> tuples = null;

    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     * 
     * @param tuples
     *            The set of tuples to iterate over
     * 一个描述这张表的tupledesc，校验所有的tuples是否符合这张表的结构
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td))
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
        }
    }

    public void open() {
        i = tuples.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() {
        return i.next();
    }

    public void rewind() {
        close();
        open();
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        i = null;
    }
}
