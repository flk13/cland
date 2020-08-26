package com.rynuk.cland.service.local;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public interface Action {
    boolean run(String urlFilePath, int progress);

    int report();

    void reportResult(int result);
}
