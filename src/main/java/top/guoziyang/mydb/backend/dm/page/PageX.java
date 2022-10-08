package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * @author
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移，剩余位置都是实际存储的数据
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFreeSpaceOffset(raw, OF_DATA);
        return raw;
    }

    /**
     * FSO: Free Space Offset
     */
    private static void setFreeSpaceOffset(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取page的FreeSpaceOffset
     */
    public static short getFreeSpaceOffset(Page page) {
        return getFreeSpaceOffset(page.getData());
    }

    private static short getFreeSpaceOffset(byte[] raw) {
        // 获取已经使用的缓存空间
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将raw插入page中，返回插入位置
     */
    public static short insert(Page page, byte[] raw) {
        // 设置当前为脏数据页
        page.setDirty(true);
        // 获取当前数据页已经使用的偏移量
        short offset = getFreeSpaceOffset(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        // 更新FreeSpaceOffset
        setFreeSpaceOffset(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 获取页面的空闲空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFreeSpaceOffset(pg.getData());
    }

    /**
     * 将raw插入page中的offset位置，并将page的offset设置为较大的offset
     * 主要是在数据库崩溃的状态下使用的，将
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFreeSpaceOffset = getFreeSpaceOffset(page.getData());
        if(rawFreeSpaceOffset < offset + raw.length) {
            setFreeSpaceOffset(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 将raw插入page中的offset位置，不更新update
     * 主要是在数据库崩溃的状态下使用的，恢复例程以及修改数据使用
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
}
