package top.guoziyang.mydb.client;

import java.util.Scanner;

public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 运行一个Client命令行
     */
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                // 退出
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    // 运行，先把输入的查询指令发送到Server端，再把receive到的数据发送回来
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            sc.close();
            client.close();
        }
    }
}
