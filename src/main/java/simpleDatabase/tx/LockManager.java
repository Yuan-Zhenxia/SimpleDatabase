package simpleDatabase.tx;

import simpleDatabase.cache.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public class LockManager {

    /**
     * 保存每一页的锁信息
     */
    private Map<PageId, List<LockState>> lockStatePool;

    /**
     * 事务id为key，pageId表示正在等待的页，
     */
    private Map<TransactionId, PageId> waitList;

    public LockManager() {
        lockStatePool = new ConcurrentHashMap<>();
        waitList = new ConcurrentHashMap<>();
    }

    /**
     * if tid already has a read lock on certain pid, return true
     * if tid already has a write lock on certain pid, or it is allowed to get a write lock on the pid, return true
     * if tid cannot has a write lock on the pid now, return false;
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean grantSLock(TransactionId tid, PageId pid) {

    }


}
