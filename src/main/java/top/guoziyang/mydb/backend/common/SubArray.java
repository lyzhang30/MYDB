package top.guoziyang.mydb.backend.common;

/**
 * 用来规定这个数组的可使用范围
 * @author 数组
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
