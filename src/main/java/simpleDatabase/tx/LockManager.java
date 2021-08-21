package simpleDatabase.tx;

import simpleDatabase.basic.Permissions;
import simpleDatabase.cache.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * finished on 21 Aug. 2021
 *
 * basically, SLock refers to share lock; XLock refers to exclusive lock
 *
 * several transactions can read the same data concurrently,
 * so if one page has a XLock, then there should not be any other SLock except the transaction's own SLock
 *
 * SLock can only add to the page that contains no other XLock except for his own XLock
 * if the page has one transaction's SLock, then other transactions can only add SLock on the page
 *
 *
 */
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
     * 根据事务tid，获取当前page上面加的所有锁
     * @param tid
     * @param pid
     * @return
     */
    public synchronized LockState getLockState(TransactionId tid, PageId pid) {
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStatePool.get(pid);
        if (lockStates == null || lockStates.size() == 0) return null;
        for (LockState ls : lockStates)
            if (ls.getTid().equals(tid)) return ls;
        return null;
    }

    /**
     * 得到tid所拥有的所有锁
     * @param tid
     * @return
     */
    private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pages = new ArrayList<>();
        for (Map.Entry<PageId, List<LockState>> entry : lockStatePool.entrySet()) {
            for (LockState ls : entry.getValue()) {
                if (ls.getTid().equals(tid))
                    pages.add(entry.getKey());
            }
        }
        return pages;
    }

    /**
     * 给当前page加上锁
     * @param pid
     * @param tid
     * @param permissions
     * @return
     */
    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions permissions) {
        LockState lockState = new LockState(tid, permissions);
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatePool.get(pid);
        if (list == null) list = new ArrayList<>();
        list.add(lockState);
        lockStatePool.put(pid, list);
        waitList.remove(tid);
        return true;
    }

    /**
     * 放入waitList中
     * @param tid
     * @param pid
     * @return
     */
    private synchronized boolean wait(TransactionId tid, PageId pid) {
        waitList.put(tid, pid);
        return false;
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
        /**
         * 分享锁
         * 一个事务可以锁住多张表，所以会给多个page加上锁
         * lockStates 会记录当前page中添加的锁所对应的 事务id
         */
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStatePool.get(pid);
        if (lockStates != null && lockStates.size() != 0) {
            if (lockStates.size() == 1) {
                /**
                 * 当前page只有一个锁，加上读锁，如果是已经有写锁，直接等待
                 */
                LockState lockState = lockStates.iterator().next();
                if (lockState.getTid().equals(tid))
                    return lockState.getPermissions() == Permissions.READ_ONLY ||
                            lock(pid, tid, Permissions.READ_ONLY); /* 如果是自己的锁 */
                else
                    return lockState.getPermissions() == Permissions.READ_ONLY ?
                            lock(pid, tid, Permissions.READ_ONLY) : wait(tid, pid); /* 如果不是自己的写锁 则等待 */
            } else {
                /**
                 * 多个锁,如果tid拥有读写锁，那么直接返回true
                 * 如果两个锁 都属于非tid事务，那么
                 */
                for (LockState ls : lockStates) {
                    if (ls.getPermissions() == Permissions.READ_WRITE) {
                        // 有写锁
                        return ls.getTid().equals(tid) /* 如果是自己的写锁，返回true */
                                || wait(tid, pid); /* 如果不是自己的写锁，等待 */
                    } else if (ls.getTid().equals(tid))
                        return true; /* 如果是自己的读锁，返回true */
                }
                return lock(pid, tid, Permissions.READ_ONLY); /* 如果是没有写锁，并且没有自己的读锁，直接枷锁 */
            }
        } else
            return lock(pid, tid, Permissions.READ_ONLY); /* 如果当前page没有锁，直接枷锁 */
    }

    /**
     * add exclusive lock (READ_WRITE lock)
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean grantXLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStatePool.get(pid);
        if (lockStates != null && lockStates.size() != 0) {
            if (lockStates.size() == 1) {
                LockState ls = lockStates.iterator().next();
                // 如果是自己的写锁，返回true；如果不是则等待；如果是自己的读锁，则加写锁
                return ls.getTid().equals(tid) ? ls.getPermissions() == Permissions.READ_WRITE
                        || lock(pid, tid, Permissions.READ_WRITE) : wait(tid, pid);
            } else {
                if (lockStates.size() == 2) { /* 如果是有两个锁，读写锁都是属于当前事务tid的，那么返回true；如果两个读写锁都不属于tid，那么wait；如果有多个读锁，wait */
                    for (LockState ls : lockStates) {
                        if (ls.getTid().equals(tid) && ls.getPermissions() == Permissions.READ_WRITE) return true;
                    }
                }
                return wait(tid, pid); /* 别人的读写锁存在，得wait */
            }
        } else {
            return lock(pid, tid, Permissions.READ_WRITE); /* 如果pid上面没有任何锁，那么可以直接加写锁 */
        }

    }

    /**
     * 释放锁 release lock
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean unlock(TransactionId tid, PageId pid) {
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStatePool.get(pid);

        if (lockStates == null || lockStates.size() == 0) return false; /* 如果是空，则返回false */

        LockState lockState = getLockState(tid, pid);
        if (lockState == null) return false; /* 如果根据tid获取不到相应的锁，返回false */
        lockStates.remove(lockState);
        lockStatePool.put(pid, lockStates); /* update cache */
        return true;
    }

    /**
     * 根据tid释放所有该事务持有的锁
     * @param tid
     */
    public synchronized void releaseAllLocksByTid(TransactionId tid) {
        List<PageId> toBeReleased = getAllLocksByTid(tid);
        for (PageId pid : toBeReleased) unlock(tid, pid);
    }


    /**
     * 关于死锁的检测和处理
     * things about deadlock
     *
     * 通过检测依赖图来判断是否存在环，如果存在环则说明陷入死锁
     *
     * tid想要给pid的page加锁，需要先检查page上所有的加了锁的事务txid 是否想要给tid所拥有的page枷锁
     * 意思是 tid想给txid的资源枷锁，txid却想给tid的资源枷锁，所以会产生死锁
     */
    public synchronized boolean dealLockDetect(TransactionId tid, PageId pid) {
        List<LockState> lockStates = lockStatePool.get(pid);
        if (lockStates == null || lockStates.size() == 0) return false;
        List<PageId> pidsByTid = getAllLocksByTid(tid); // 找出所有tid这个事务已经添加的锁

        for (LockState ls : lockStates) {
            TransactionId txid = ls.getTid();
            if (!txid.equals(tid)) { /* 判断该page上锁的持有者是否和事务tid相同，如果不同，则需要检测死锁 */
                if (isWaiting(txid, pidsByTid, tid)) return true; // 判断此page上的所有加了锁的事务txid 是否在等待tid所拥有的锁的page
            }
        }
        return false;
    }

    /**
     * 判断tid是否在等待pids中的某个资源，坦白说逻辑有点绕
     *
     * 举例：
     * 事务A有page1资源，想给page2资源加锁
     *
     * 如果page2上的锁对应的事务B（B拥有了page2），想给page1加锁，那么他们就发生了死锁
     *
     * 如果page2上的锁对应的事务B（B拥有了page2），想给page3加锁；
     * 并且page3的拥有者想给page1加锁，那么这就发生了间接死锁
     *
     * @param tid
     * @param pids
     * @param toDelete
     * @return
     */
    public synchronized boolean isWaiting(
            TransactionId tid, List<PageId> pids /* 前一个事务拥有的pages */, TransactionId toDelete) {
        PageId waitPage = waitList.get(tid); /* 获取tid所需要的正在等待的资源；即tid想加锁却加不上的page */
        if (waitPage == null) return false;
        for (PageId pid : pids)
            if (pid.equals(waitPage)) return true; /* 这里表明直接发生了死锁 */

        List<LockState> lockStates = lockStatePool.get(waitPage); /* 获取该page上所有的事务 */
        if (lockStates == null || lockStates.size() == 0) return false; /* 该page没有事务拥有它 */

        for (LockState ls : lockStates) {
            TransactionId owner = ls.getTid();
            /* 待检查的事务 toDelete和他拥有的资源pids；每次都检查是否间接造成死锁 */
            if (!owner.equals(toDelete))
                if (isWaiting(owner, pids, toDelete)) return true;
        }
        return false; /* 表明没有事务直接或者间接等待资源 */
    }

}
