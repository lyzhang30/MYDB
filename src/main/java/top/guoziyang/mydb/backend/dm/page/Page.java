package top.guoziyang.mydb.backend.dm.page;

/**
 * 页面的接口定义
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
