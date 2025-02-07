package top.guoziyang.mydb.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.server.Server;
import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.vm.VersionManager;
import top.guoziyang.mydb.backend.vm.VersionManagerImpl;
import top.guoziyang.mydb.common.Error;

public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        // 第一次运行时需要传入的数据库文件路径，例如可以传入 -open D:/mydb/mydb
        options.addOption("open", true, "-open DBPath");
        // 创建数据库文件的路径
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        // 解析器
        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = parser.parse(options, args);
        // 打开数据库
        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        // 创建一个数据库
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /**
     * 初始化一个数据库，初始化的顺序是<br/>
     * 1.首先是初始化Transaction Manager
     * 2.然后初始化一个Data Manager
     * 3.再初始化一个Version Manager
     * 4.初始化Table Manager
     * @param path 路径
     */
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
