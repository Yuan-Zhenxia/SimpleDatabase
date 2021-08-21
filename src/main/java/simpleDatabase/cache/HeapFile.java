package simpleDatabase.cache;

import simpleDatabase.basic.Database;
import simpleDatabase.basic.Permissions;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.iterator.DbFileIterator;
import simpleDatabase.tx.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * finished
 *
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 *
 * tuples存储在pages中， HeapFile则是HeapPages的集合
 */
public class HeapFile implements DbFile {

    private File file;

    private int numPage;

    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        // 根据偏移量计算对应的页数
        numPage = (int) (file.length() / BufferPool.DEFAULT_PAGE_SIZE);
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableId somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javaDocs

    /**
     * 根据pageId 从磁盘读取一个页，
     * 该方法只能在BufferPool被调用，这样才能保证数据正确
     **/
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getTableId() != getId()) throw new IllegalArgumentException();

        Page page = null;
        byte[] data = new byte[BufferPool.DEFAULT_PAGE_SIZE];

        // RandomAccessFile 可以随机读取
        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            int pos = pid.getPageNumber() * BufferPool.DEFAULT_PAGE_SIZE;
            raf.seek(pos);
            raf.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javaDocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.DEFAULT_PAGE_SIZE);
            byte[] data = page.getPageData();
            raf.write(data);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javaDocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // TODO about transaction
        ArrayList<Page> dirtyPages = new ArrayList<>();
        for (int i = 0; i < numPages(); ++i) {
            HeapPageId hPId = new HeapPageId(getId(), i);
            HeapPage heapPage = null;
            try {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, hPId, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (heapPage.getNumEmptySlots() != 0) {
                heapPage.insertTuple(t); /* 还有空的位置能放入 */
                heapPage.markDirty(true, tid);
                dirtyPages.add(heapPage);
                break;
            }
        }
        if (dirtyPages.size() == 0) {
            HeapPageId newPid = new HeapPageId(getId(), numPages());
            HeapPage blankPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
            numPage++;
            writePage(blankPage);
            HeapPage newPage = null;
            try {
                newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
            } catch (InterruptedException e) { e.printStackTrace(); }
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            dirtyPages.add(newPage);
        }
        return dirtyPages;
    }

    // see DbFile.java for javaDocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage dirtyPage = null;
        for (int i = 0; i < numPages(); ++i) {
            if (i == pid.getPageNumber()) {
                try {
                    dirtyPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dirtyPage.deleteTuple(t);
                dirtyPage.markDirty(true, tid);
            }
        }
        if (dirtyPage == null) throw new DbException("tuple " + t + " is not int the table");
        return dirtyPage;
    }

    // see DbFile.java for javaDocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> tuplesInPage;

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage page = null;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuplesInPage == null) return false; /* 已经关闭了 */
            if (tuplesInPage.hasNext()) return true; /* 表示当前页还有没有被遍历玩的 */
            if (pagePos < numPages() - 1) {
                HeapPageId pid = new HeapPageId(getId(), ++pagePos);
                tuplesInPage = getTuplesInPage(pid);
                return tuplesInPage.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException("no tuple left");
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            pagePos = 0;
            tuplesInPage = null;
        }
    }

}