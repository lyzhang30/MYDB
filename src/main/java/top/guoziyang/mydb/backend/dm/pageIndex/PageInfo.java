package top.guoziyang.mydb.backend.dm.pageIndex;

/**
 * 记录页码和空闲空间大小的信息
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
