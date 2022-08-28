package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

/**
 * @author 页面接口的实现类
 */
public class PageImpl implements Page {
    /**
     * 当前页码，从1开始
     */
    private int pageNumber;
    /**
     * 当前面保存的数据
     */
    private byte[] data;
    /**
     * 当前页面的数据是否为脏数据，当在缓存的这个资源的引用为0，需要驱逐的时候，我们就需要把这
     * 个脏页面写回到磁盘
     */
    private boolean dirty;
    /**
     * 页面的锁
     */
    private Lock lock;
    
    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }

}
