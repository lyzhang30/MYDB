package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

public class TransactionManagerTest {

    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 1;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        tmger = TransactionManager.create("D:/mydb/tranmger_test");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i ++) {
            Runnable r = this::worker;
            new Thread(r).start();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new File("D:/mydb/tranmger_test.xid").delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if (op == 0) {
                lock.lock();
                if (!inTrans) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                if (transCnt > 0) {
                    long xid = (random.nextInt(Integer.MAX_VALUE) % transCnt) + 1;
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
            System.out.println("第" + (i+1) +"\t op==>" + op +"\ttransXID==>" + transXID + "\t" + "inTrans ==>" + inTrans);
        }

        cdl.countDown();
    }
}
