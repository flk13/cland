package com.rynuk.cland.zk;

import com.rynuk.cland.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 用于执行不依赖顺序性的操作，防止EventThread阻塞，提高执行效率
 *
 * @author rynuk
 * @date 2020/8/11
 */
public class AsyncOpThreadPool {
    private final ExecutorService threadPool;

    private static AsyncOpThreadPool asyncOpThreadPool;

    private AsyncOpThreadPool() {
        threadPool = Executors.newFixedThreadPool(Configuration.INSTANCE.LOCAL_ASYNC_THREAD_NUM);
    }

    public static synchronized AsyncOpThreadPool getInstance() {
        if (asyncOpThreadPool == null) {
            asyncOpThreadPool = new AsyncOpThreadPool();
        }
        return asyncOpThreadPool;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }
}
