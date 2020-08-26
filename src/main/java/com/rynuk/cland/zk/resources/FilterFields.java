package com.rynuk.cland.zk.resources;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public interface FilterFields extends AdditionalFields {
    boolean mount();

    boolean unmount();

    Object getFilterInfo();
}
