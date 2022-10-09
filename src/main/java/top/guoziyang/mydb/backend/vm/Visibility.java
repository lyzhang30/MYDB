package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读已提交
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 由xmin事务创建
        if (xmin == xid && xmax == 0) {
            return true;
        }
        // 如果这个事务已经提交，但是未删除或者这个事务由一个未提交的事务删除
        if (tm.isCommitted(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != xid) {
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 由xmin事务创建，未删除
        if (xmin == xid && xmax == 0) {
            return true;
        }
        // 这个事务由一个已提交的事务创建并且这个事务小于Ti，如果这个事务在Ti开始前就已经提交
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 这个事务尚未被删除
            if (xmax == 0) {
                return true;
            }
            // 由其他事务删除
            if (xmax != xid) {
                // 该事务尚未提交或在Ti之后才开始，这个事务在Ti开始前还没提交
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
