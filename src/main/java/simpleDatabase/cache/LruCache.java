package simpleDatabase.cache;

import simpleDatabase.exception.CacheException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LruCache<K, V> {


    protected class Node {
        Node prev, ne;
        K key;
        V val;

        Node (K k, V v) {
            this.key = k;
            this.val = v;
        }
    }

    protected HashMap<K, Node> cache;

    protected int capacity;

    protected Node head, tail;

    public LruCache(int c) {
        this.capacity = c;
        cache = new HashMap<>(c);
        head = new Node(null, null);
    }


    public void delNode(Node node) {
        if (node.ne == null) node.prev.ne = null;
        else {
            node.prev.ne = node.ne;
            node.ne.prev = node.prev;
        }
    }

    public void addFirst(Node node) {
        Node first = this.head.ne;
        this.head.ne = node;
        node.ne = first;
        node.prev = this.head;
        if (first == null) tail = node;
        else first.prev = node;
    }

    public Node removeLast() {
        Node nTail = tail.prev;
        tail.prev = null;
        nTail.ne = null;
        tail = nTail;
        return nTail;
    }

    public synchronized boolean isCached(K k) {
        return cache.containsKey(k);
    }

    /**
     * 把key和val组成的node节点放入链表中。更新cache中的缓存
     * 如果return null 那么就是没有删除旧元素。
     * @param k
     * @param v
     * @return
     * @throws CacheException
     */
    public synchronized V put(K k, V v) throws CacheException {
        if (k == null || v == null) throw new IllegalArgumentException();
        if (isCached(k)) {
            Node node = cache.get(k);
            Node n = cache.get(k);
            n.val = v;
            delNode(n);
            addFirst(n);
            return null;
        } else {
            V del = null;
            if (cache.size() == capacity) {
                del = cache.remove(tail.key).val;
                removeLast();
            }
            Node node = new Node(k, v);
            addFirst(node);
            cache.put(k, node);
            return del;
        }
    }

    /**
     * 加了synchronized后，读写都是最新版本，串行化
     * @param key
     * @return
     */
    public synchronized V get(K key) {
        if (isCached(key)) {
            // put the recently used node in the first
            Node node = cache.get(key);
            if (tail == node && node.prev != head)
                tail = node.prev;

            delNode(node);
            addFirst(node);
            return node.val;
        }
        return null;
    }

    public Iterator<V> iterator() {return new LruIterator();}

    /**
     * 如果不加synchronized 就好像java自带的 ，就会出现ConcurrentModificationException
     */
    public class LruIterator implements Iterator<V> {

        Node n = head;

        @Override
        public synchronized boolean hasNext() {
            return n.ne != null;
        }

        @Override
        public synchronized V next() {
            if (!hasNext()) throw new NoSuchElementException();
            n = n.ne;
            return n.val;
        }
    }

}
