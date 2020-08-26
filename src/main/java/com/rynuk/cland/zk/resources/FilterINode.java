package com.rynuk.cland.zk.resources;

import java.time.Instant;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class FilterINode extends INode implements FilterFields {
    public FilterINode(Instant createTime, Instant modificationTime, String group, int uniqueMarkup) {
        super(createTime, modificationTime, group, uniqueMarkup);
    }

    @Override
    public boolean mount() {
        return false;
    }

    @Override
    public boolean unmount() {
        return false;
    }

    @Override
    public Object getFilterInfo() {
        return null;
    }
}

