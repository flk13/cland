package com.rynuk.cland.check;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.saver.dfs.HDFSManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class SelfTest {
    private static final Logger logger = LoggerFactory.getLogger(SelfTest.class);

    private static Configuration configuration = Configuration.INSTANCE;

    /**
     * 检查某个class是否已经在运行
     *
     * @param className
     * @return
     */
    public static boolean checkRunning(String className) {
        boolean result = false;
        int counter = 0;
        try {
            Process process = Runtime.getRuntime().exec("jps");
            InputStreamReader iR = new InputStreamReader(process.getInputStream());
            BufferedReader input = new BufferedReader(iR);
            String line;
            while ((line = input.readLine()) != null) {
                if (line.matches(".*" + className)) {
                    counter++;
                    if (counter > 1) {
                        result = true;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 检查ZooKeeper的连接状态和它的Znode目录树
     *
     * @param
     * @return
     */
    public static CuratorFramework checkAndGetZK() {
        CuratorFramework client;
        try {
            RetryPolicy retryPolicy =
                    new ExponentialBackoffRetry(configuration.ZK_RETRY_INTERVAL, configuration.ZK_RETRY_TIMES);
            client = CuratorFrameworkFactory
                    .newClient(configuration.ZK_CONNECT_STRING
                            , configuration.ZK_SESSION_TIMEOUT, configuration.ZK_INIT_TIMEOUT, retryPolicy);
            client.start();
            client.checkExists().forPath(ZNodeStaticSetting.TASKS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.MANAGERS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.WORKERS_PATH);
            client.checkExists().forPath(ZNodeStaticSetting.FILTERS_ROOT);
        } catch (Throwable e) {
            client = null;
            logger.error(e.getMessage());
        }
        return client;
    }

    /**
     * 检查HDFS的连接状态和它的目录树
     *
     * @return
     */
    public static DFSManager checkAndGetDFS() {
        DFSManager hdfsManager;
        try {
            hdfsManager = new HDFSManager(configuration.HDFS_SYSTEM_CONF, configuration.HDFS_SYSTEM_PATH);
            hdfsManager.exist(configuration.BLOOM_BACKUP_PATH);
            hdfsManager.exist(configuration.FINISHED_TASKS_URLS);
            hdfsManager.exist(configuration.WAITING_TASKS_URLS);
            hdfsManager.exist(configuration.NEW_TASKS_URLS);
        } catch (Throwable e) {
            hdfsManager = null;
            logger.error(e.getMessage());
        }
        return hdfsManager ;
    }
}
