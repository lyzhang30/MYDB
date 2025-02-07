package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

public class PageIndex {
    /**
     * 将一页划成40个区间
     */
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            // 上层模块使用完这个页面后，需要手动插入回来
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }


    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 算出区间号
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) {
                number ++;
            }
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                // 从PageIndex中移除， 意味着同一个页面时不允许并发写的，
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
