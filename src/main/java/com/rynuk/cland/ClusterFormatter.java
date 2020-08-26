package com.rynuk.cland;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.saver.dfs.HDFSManager;
import com.rynuk.cland.utils.Color;
import com.rynuk.cland.utils.InitLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class ClusterFormatter implements Watcher {
    private static ClusterFormatter clusterFormatter;

    private static Logger logger = LoggerFactory.getLogger(ClusterFormatter.class);

    private static Configuration configuration = Configuration.INSTANCE;

    private ZooKeeper client;

    private HDFSManager hdfsManager;

    /**
     * 初始化znode基础设置，生成3个永久的znode
     * /
     * |--- cland
     * |--- cdTasks
     * |--- cdWorkers
     * |--- cdManagers
     */
    public void initZK() throws KeeperException, InterruptedException {
        try {
            createParent(ZNodeStaticSetting.WORKERS_PATH);
            createParent(ZNodeStaticSetting.MANAGERS_PATH);
            createParent(ZNodeStaticSetting.TASKS_PATH);
            createParent(ZNodeStaticSetting.FILTERS_ROOT);
        } catch (KeeperException.NodeExistsException e) {
            // pass is ok
        }
    }

    /**
     * 初始化hdfs的目录树
     * /
     * |--- cland
     * |--- bloom
     * |--- tasks
     * |--- new urls
     * |--- waiting tasks
     * |--- finished tasks
     *
     * @throws IOException
     */
    public void initHDFS() throws IOException {
        hdfsManager.mkdirs(configuration.BLOOM_BACKUP_PATH);
        hdfsManager.mkdirs(configuration.NEW_TASKS_URLS);
        hdfsManager.mkdirs(configuration.WAITING_TASKS_URLS);
        hdfsManager.mkdirs(configuration.FINISHED_TASKS_URLS);
    }

    /**
     * 强制初始化ZooKeeper(会清除原来存在的节点)
     */
    public void formatZK() throws KeeperException, InterruptedException {
        deleteParent(ZNodeStaticSetting.WORKERS_PATH);
        deleteParent(ZNodeStaticSetting.MANAGERS_PATH);
        deleteParent(ZNodeStaticSetting.TASKS_PATH);
        deleteParent(ZNodeStaticSetting.FILTERS_ROOT);
        initZK();
    }

    /**
     * 强制初始化hdfs的目录树(会清除原来存在的文件)
     *
     * @throws IOException
     */
    public void formatHDFS() throws IOException {
        hdfsManager.delete(configuration.BLOOM_BACKUP_PATH, true);
        hdfsManager.delete(configuration.NEW_TASKS_URLS, true);
        hdfsManager.delete(configuration.WAITING_TASKS_URLS, true);
        hdfsManager.delete(configuration.FINISHED_TASKS_URLS, true);
        initHDFS();
    }

    public static synchronized ClusterFormatter getInstance() {
        if (clusterFormatter == null) {
            clusterFormatter = new ClusterFormatter();
        }
        return clusterFormatter;
    }

    private void createParent(String path)
            throws KeeperException, InterruptedException {
        client.create(path, "".getBytes(),
                OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void deleteParent(String path)
            throws KeeperException, InterruptedException {
        client.delete(path, -1);
    }

    private ClusterFormatter() {
        try {
            client = new ZooKeeper(configuration.ZK_CONNECT_STRING
                    , configuration.ZK_SESSION_TIMEOUT, this);
            hdfsManager = new HDFSManager(configuration.HDFS_SYSTEM_CONF
                    , configuration.HDFS_SYSTEM_PATH);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        InitLogger.init();
        try {
            ClusterFormatter formatter = ClusterFormatter.getInstance();
            if (args.length > 0 && args[0].equals("-f")) {

                System.out.println(Color.error("delete all old setting and  initialize?(y/n)"));
                char choice = (char) System.in.read();
                if (choice == 'y' || choice == 'Y') {
                    System.out.println("Format Zookeeper...");
                    formatter.formatZK();
                    System.out.println("Format HDFS...");
                    formatter.formatHDFS();
                    System.out.println("Done.");
                }
            } else {
                System.out.println("Init Zookeeper...");
                formatter.initZK();
                System.out.println("Init HDFS...");
                formatter.initHDFS();
                System.out.println("Done.");
            }
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}
