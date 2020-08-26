package com.rynuk.cland.zk.resources;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public interface INodeAttributes {
    boolean isDirectory();

    String getPath();

    void lock();

    void unlock();

    boolean isLocked();

    int getMarkup();

    String getGroup();
}
