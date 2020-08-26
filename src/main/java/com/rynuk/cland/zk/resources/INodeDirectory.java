package com.rynuk.cland.zk.resources;

import java.time.Instant;
import java.util.List;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class INodeDirectory implements INodeAttributes {
    private final Instant createTime;

    private final Instant modificationTime;

    private String name;

    private long permission;

    private List<INodeAttributes> children = null;

    public INodeDirectory(Instant createTime, Instant modificationTime) {
        this.createTime = createTime;
        this.modificationTime = modificationTime;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public boolean isLocked() {
        return false;
    }

    @Override
    public int getMarkup() {
        return 0;
    }

    @Override
    public String getGroup() {
        return null;
    }
}
