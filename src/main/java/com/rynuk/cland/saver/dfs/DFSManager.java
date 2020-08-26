package com.rynuk.cland.saver.dfs;

import java.io.IOException;
import java.util.List;

/**
 * 分布式文件管理
 * @author rynuk
 * @date 2020/7/27
 */
public interface DFSManager {
    /**
     * 从分布式文件系统中下载文件
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    void downloadFile(String src, String dst) throws IOException;

    /**
     * 向分布式文件系统中上传文件
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    void uploadFile(String src, String dst) throws IOException;

    /**
     * 创建新的文件夹
     * 注：不覆盖已经存在的文件夹
     *
     * @param src
     * @return
     * @throws IOException
     */
    boolean mkdirs(String src) throws IOException;

    /**
     * 删除文件或文件夹
     *
     * @param path      路径
     * @param recursive 是否递归
     * @throws IOException
     */
    void delete(String path, boolean recursive) throws IOException;

    /**
     * 移动文件或文件夹
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    void move(String src, String dst) throws IOException;

    /**
     * 判断文件或者文件夹是否存在
     *
     * @param src
     * @return
     * @throws IOException
     */
    boolean exist(String src) throws IOException;

    /**
     * 列出文件夹下的文件
     * 注：不列出文件夹
     *
     * @param src
     * @param recursive 是否递归
     * @return
     * @throws IOException
     */
    List<String> listFiles(String src, boolean recursive) throws IOException;

    /**
     * 断开与分布式文件系统的客户端连接
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * 获得文件最后一次修改的时间
     *
     * @param src
     * @return
     * @throws IOException
     */
    long getFileModificationTime(String src) throws IOException;

    /**
     * 获取文件长度
     *
     * @param src
     * @return
     * @throws IOException
     */
    long getFileLen(String src) throws IOException;
}
