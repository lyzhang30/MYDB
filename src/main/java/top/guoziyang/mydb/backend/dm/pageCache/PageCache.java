package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public interface PageCache {
    
    int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNo) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPageNo);
    int getPageNumber();
    void flushPage(Page page);

    /**
     * 创建一个db存储数据的文件
     */
    static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    /**
     * 打开一个db存储的文件
     */
    static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
}
