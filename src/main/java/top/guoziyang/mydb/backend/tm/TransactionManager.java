package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

/**
 *  事务管理器
 *
 *@author https://ziyang.moe/article
 *@date 2022/8/25
 */
public interface TransactionManager {
    /**
     * 开启一个事务
     */
    long begin();

    /**
     * 提交一个事务
     */
    void commit(long xid);

    /**
     * 回滚一个事务
     */
    void abort(long xid);

    /**
     * 查询一个事务的状态是否是正在进行的状态
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务的状态是否是已提交
     */
    boolean isCommitted(long xid);

    /**
     * 查询一个事务的状态是否是已取消
     */
    boolean isAborted(long xid);

    /**
     * 关闭TM
     */
    void close();

    /**
     * 创建一个数据库
     * @param path 创建一个数据库的路径
     * @return 返回一个事务管理器的实现类
     */
     static TransactionManagerImpl create(String path) {
        // 如果这个路径下的文件下不存在的话，我们就先创建这个文件夹，然后再进行生成数据库
        int index = path.lastIndexOf("/");
        String dataBasePath = path.substring(0, index);
        File dataBasePathDirectory = new File(dataBasePath);
        File writeFile = new File(path + TransactionManagerImpl.XID_SUFFIX);
        // 目录不存在就先创建目录
        try {

            if (!dataBasePathDirectory.exists()) {
                dataBasePathDirectory.mkdirs();
            }
            if(!writeFile.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if(!writeFile.canRead() || !writeFile.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(writeFile, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开数据库
     * @param path 数据库路径
     * @return 返回一个事务管理器
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
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

        return new TransactionManagerImpl(raf, fc);
    }
}
