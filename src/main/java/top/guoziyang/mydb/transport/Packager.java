package top.guoziyang.mydb.transport;

import java.util.Arrays;

public class Packager {
    private Transporter transpoter;
    private Encoder encoder;

    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        Package decode = encoder.decode(data);
        String originalData = new String(decode.getData());
        System.out.println("sql==>" + originalData);
        transpoter.send(data);
    }

    public Package receive() throws Exception {
        // 接收数据
        byte[] data = transpoter.receive();
        // 解码数据
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transpoter.close();
    }
}
