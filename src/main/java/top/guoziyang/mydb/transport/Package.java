package top.guoziyang.mydb.transport;

public class Package {
    /**
     * 数据包
     */
    byte[] data;
    /**
     * 异常信息
     */
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
