package simpleDatabase.cache;

import simpleDatabase.basic.Permissions;
import simpleDatabase.exception.DbException;
import simpleDatabase.exception.TransactionAbortedException;
import simpleDatabase.basic.Database;
import simpleDatabase.tx.LockManager;
import simpleDatabase.tx.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * 缓冲池也对锁负责，当一个事物使用了一个page，缓冲池会检查这个事务有正确的读/写锁
 *
 * 为了线程安全，所有属性都是final
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    /** 每个page的字节数, 一般常见默认页大小 4kb **/
    public static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * 页的最大数量
     */
    public final int PAGES_NUM;

    private PageLruCache pageCache; // lru

    private final LockManager lockManager;

    private final long SLEEP_TIME; // 事务竞争锁需要等待的时间


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        PAGES_NUM = numPages;
        pageCache = new PageLruCache(PAGES_NUM);
        lockManager = new LockManager();
        SLEEP_TIME = 300; /* use loop to try to get the lock, if too small, then cost lots of computer resources */
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException, InterruptedException {
        // 如果当前page被其他事务占有，那么无法获取该page 必须while等待
        boolean tryAcquire = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                : lockManager.grantXLock(tid, pid); /* 根据所需要的锁类型来分配不同的锁 */

        while (!tryAcquire) {
            // 没有获取锁成功
            if (lockManager.dealLockDetect(tid, pid)) throw new TransactionAbortedException();
            Thread.sleep(SLEEP_TIME);
            tryAcquire = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                    : lockManager.grantXLock(tid, pid); /* 再次判断是否能获取锁 */
        }

        if (pageCache.isCached(pid)) return pageCache.get(pid);
        // 未命中
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
        HeapPage newPage = (HeapPage) table.readPage(pid); // WARN
        Page removedPage = pageCache.put(pid, newPage);
        if (removedPage != null) { /* 不等于null 表示删除了一个老的缓存 */
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        if (!lockManager.unlock(tid, pid))
            throw new IllegalArgumentException("try to release page failed");
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.getLockState(tid, p) != null;
    }

    /**
     * 在事务回滚前，首先需要恢复该数据对page造成的改变
     * @param tid
     */
    public synchronized void revertTransactionAction(TransactionId tid) {
        Iterator<Page> it = pageCache.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            /* 假如这是脏页，且该脏页的数据是由该事务修改的，那么需要从磁盘中恢复原来的数据 */
            if (p.isDirty() != null && p.isDirty().equals(tid))
                pageCache.reCachePage(p.getId()); /* 从磁盘恢复该page的数据 */
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        lockManager.releaseAllLocksByTid(tid);
        if (commit)
            flushPages(tid);
        else
            revertTransactionAction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> dirtyPages = table.insertTuple(tid, t);
        for (Page p : dirtyPages) p.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        Page dirtyPage = table.deleteTuple(tid, t);
        dirtyPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Iterator<Page> it = pageCache.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null)
                flushPage(p);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        // TODO
    }

    /**
     * flush page to the disk
     * @param page
     * @throws IOException
     */
    private synchronized  void flushPage(Page page) throws IOException {
        // some code goes here
        HeapPage dirtyPage = (HeapPage) page;
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(page.getId().getTableId());
        table.writePage(dirtyPage); /* 使用table把脏页数据写到磁盘上，然后标记为干净页 */
        dirtyPage.markDirty(false, null);
    }

    /**
     * 把当前事务tid相关的page刷盘
     * Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        Iterator<Page> it = pageCache.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                flushPage(p);
                if (p.isDirty() == null) // 刷盘成功后，保存oldData 用来回滚
                    p.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     *
     * realize this in LRUCache
     */
    @Deprecated
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //TODO
    }

}
