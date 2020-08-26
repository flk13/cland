package com.rynuk.cland.zk.resources;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class INode implements INodeAttributes {
    private final Instant createTime;

    private final Instant modificationTime;

    private AtomicBoolean lock = new AtomicBoolean(false);

    private int name;

    private String group;

    private long permission;

    private String path;

    public INode(Instant createTime, Instant modificationTime, String group, int uniqueMarkup) {
        this.createTime = createTime;
        this.modificationTime = modificationTime;
        this.name = uniqueMarkup;
        this.group = group;
    }

    public long getCreateTime() {
        return createTime.toEpochMilli();
    }

    public long getLastModificationTime() {
        return modificationTime.toEpochMilli();
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void lock() {
        lock.set(true);
    }

    @Override
    public void unlock() {
        lock.set(false);
    }

    @Override
    public boolean isLocked() {
        return lock.get();
    }

    @Override
    public int getMarkup() {
        return name;
    }

    @Override
    public String getGroup() {
        return group;
    }
}
