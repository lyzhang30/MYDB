package top.guoziyang.mydb.backend.utils;

public class Panic {
    /**
     * 用来停止程序，如果不是可以处理的程序就直接停掉当前这个运行的进程。
     * @param err 报错信息
     */
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
