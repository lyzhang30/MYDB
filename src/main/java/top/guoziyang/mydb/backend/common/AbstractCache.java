package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    /**
     * 实际缓存的数据
     */
    private final HashMap<Long, T> cache;
    /**
     * 元素的引用个数
     */
    private final HashMap<Long, Integer> references;
    /**
     * 正在获取某资源的线程
     */
    private final HashMap<Long, Boolean> getting;
    /**
     * 缓存的最大缓存资源数
     */
    private final int maxResource;
    /**
     * 缓存中元素的个数
     */
    private int count = 0;
    private final Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            // 查看是否有进程正在获取这个资源
            if (getting.containsKey(key)) {
                System.out.println("key==>" + key + "的数据正在被其他线程获取");
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回，引用数 + 1
                T obj = cache.get(key);
                System.out.println("命中缓存==>" + obj);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源，如果资源已经达到缓存的最大资源值
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;
            // 当前这个资源正在被某线程获取
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        // 读取完资源后就把正在获取的资源删除掉，加入到缓存中
        getting.remove(key);
        cache.put(key, obj);
        // 设置初始引用值
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }

    /**
     * 当某个线程对这个资源不在使用后，就释放对资源的引用，当引用归零后，就驱逐这个资源
     *
     */
    protected void release(long key) {
        lock.lock();
        try {
            // 减掉当前这个引用，得到当前这个数据的被引用的次数
            int ref = references.get(key) - 1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                // 从引用中移除
                references.remove(key);
                // 从缓存中移除
                cache.remove(key);
                // 元素个数减少
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
