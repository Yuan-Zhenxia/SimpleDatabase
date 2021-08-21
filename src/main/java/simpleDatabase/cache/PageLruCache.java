package simpleDatabase.cache;

import simpleDatabase.basic.Database;
import simpleDatabase.exception.CacheException;

public class PageLruCache extends LruCache<PageId, Page> {

    public PageLruCache(int c) {super(c);}

    @Override
    public synchronized Page put(PageId k, Page v) throws CacheException {
        if (k == null || v == null) throw new IllegalArgumentException();
        if (isCached(k)) {
            Node node = cache.get(k);
            node.val = v; // update its value and then put it back to the cache
            delNode(node);
            addFirst(node);
            return null;
        } else {
            Page del = null;
            if (cache.size() == capacity) {
                Page toDelete = null;
                Node n = tail;
                while ((toDelete = n.val).isDirty() != null) {
                    n = n.prev;
                    if (n == head) throw new CacheException("" +
                            "Page Cache is full and all pages are dirty");
                }
                // delete the node in the linkedlist and in the cache
                delPage(toDelete.getId());
                del = cache.remove(toDelete.getId()).val;
            }
            Node node = new Node(k, v);
            addFirst(node);
            cache.put(k, node);
            return del;
        }
    }

    private void delPage(PageId id) {
        if (!isCached(id)) throw new IllegalArgumentException();
        Node toDelete = head;
        // 从头开始遍历，如果没找到，则啥也不做
        while (!(toDelete = toDelete.ne).key.equals(id));
        if (toDelete == tail) removeLast();
        else {
            toDelete.ne.prev = toDelete.prev;
            toDelete.prev.ne = toDelete.ne;
        }
    }

    public synchronized void reCachePage(PageId pid) {
        if (!isCached(pid)) throw new IllegalArgumentException();

        // load this page from the disk
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
        HeapPage oriPage = (HeapPage) table.readPage(pid);
        Node node = new Node(pid, oriPage);
        cache.put(pid, node);
        Node toDelete = head;
        while(!(toDelete = toDelete.ne).key.equals(pid)) ;
        node.prev = toDelete.prev;
        node.ne = toDelete.ne;
        toDelete.prev.ne = node;

        // 看是否重新缓存当是否是tail节点， 需要修改tail指针当
        if (toDelete.ne != null) toDelete.ne.prev = node;
        else tail = node;
    }

}
