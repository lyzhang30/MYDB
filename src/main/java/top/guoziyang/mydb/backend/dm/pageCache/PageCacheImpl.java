package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    /**
     * 计算当前数据库文件有多少页，新增一个页面时就自动增加
     */
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return get((long) pageNo);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int)key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pageNo, buf.array(), this);
    }

    /**
     * 从缓存中释放这个页数据时，需要判断当前页面是否为脏页面，再判断是否需要写回文件系统
     */
    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }
    @Override
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pageNo) {
        return (long) (pageNo - 1) * PAGE_SIZE;
    }
    
}
