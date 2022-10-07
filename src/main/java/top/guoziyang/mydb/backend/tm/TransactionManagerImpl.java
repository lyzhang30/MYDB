package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * @author 大勇
 */
public class TransactionManagerImpl implements TransactionManager {

    /**
     * XID文件头长度
     */
    static final int LEN_XID_HEADER_LENGTH = 8;
    /**
     * 每个事务的占用长度
     */
    private static final int XID_FIELD_SIZE = 1;
    /**
     * 事务的三种状态，0是正常运行，1是已提交，2是已经回滚
     */
    private static final byte FIELD_TRAN_ACTIVE   = 0;
	private static final byte FIELD_TRAN_COMMITTED = 1;
	private static final byte FIELD_TRAN_ABORTED  = 2;

    /**
     * 超级事务，永远为committed状态
     */
    public static final long SUPER_XID = 0;
    /**
     * XID 文件后缀
     */
    static final String XID_SUFFIX = ".xid";
    
    private final RandomAccessFile file;
    private final FileChannel fc;
    private long xidCounter;
    private final Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        // 根据文件头的8字节数字可以反推文件的理论长度，与文件的实际长度做对比
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status，即在事务的3个提交状态中选择一个0，1，2
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void increaseXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 方法，强制同步缓存内容到文件中，类似flush()方法
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个事务，并返回XID
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            // 对事务计数器的id加1
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            increaseXIDCounter();
            return xid;
        } finally {
            // 释放锁
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务，更新状态成1
     */
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务，更新状态为x
     */
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于status状态
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID){
            return true;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
