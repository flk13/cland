package com.rynuk.cland.filter;

import java.io.IOException;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public interface Filter {
    /**
     * 将URI放入filter
     *
     * @param str
     * @return
     */
    boolean put(String str);

    /**
     * 判断URI是否已存在于filter
     *
     * @param str
     * @return
     */
    boolean exist(String str);

    /**
     * 序列化filter并存储到本地磁盘
     *
     * @param dst
     * @return
     * @throws IOException
     */
    String save(String dst) throws IOException;

    /**
     * 反序列化在本地磁盘上的filter文件
     *
     * @param src
     * @throws IOException
     */
    void load(String src) throws IOException;
}
