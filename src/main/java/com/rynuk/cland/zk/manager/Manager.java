package com.rynuk.cland.zk.manager;

import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.exception.ClandRuntimeException;
import com.rynuk.cland.filter.Filter;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.utils.Async;
import com.rynuk.cland.utils.MD5Maker;
import com.rynuk.cland.zk.AsyncOpThreadPool;
import com.rynuk.cland.zk.task.Epoch;
import com.rynuk.cland.zk.task.TaskManager;
import com.rynuk.cland.zk.worker.WorkersWatcher;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager用于管理整个事务
 * 其中active_manager为活动的Server，而standby manager
 * 监听active manager，一旦活动节点失效则接管其工作
 *
 * @author rynuk
 * @date 2020/8/11
 */
public class Manager {
    /**
     * Manager的状态
     */
    public enum Status {
        /*
            INITIALIZING: 刚初始化，还未进行选举
         */
        INITIALIZING,
        /*
            ELECTED: 主节点
        */
        ELECTED,
        /*
            NOT_ELECTED: 从节点
        */
        NOT_ELECTED,
        /*
            RECOVERING: 检测到主节点死亡，尝试恢复中
        */
        RECOVERING
    }

    public static Manager manager;

    private static final Logger logger = LoggerFactory.getLogger(Manager.class);

    private static Configuration configuration = Configuration.INSTANCE;

    private CuratorFramework client;

    private String serverId;

    private String managerPath;

    private Status status;

    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);

    private ExecutorService asyncOpThreadPool = AsyncOpThreadPool.getInstance().getThreadPool();

    private WorkersWatcher workersWatcher;

    private TaskManager taskManager;

    private DFSManager dfsManager;

    private Map<String, String> workersMap = new HashMap<>();

    /* 未完成的任务指RUNNING状态的任务 */
    private Map<String, Epoch> unfinishedTaskMap = new HashMap<>();

    private Filter filter;

    private Manager(CuratorFramework client, String serverId,
                    DFSManager dfsManager, Filter filter) {
        this.client = client;
        this.serverId = serverId;
        this.dfsManager = dfsManager;
        this.filter = filter;
        taskManager = new TaskManager(client);
        workersWatcher = new WorkersWatcher(client);
        status = Status.INITIALIZING;
        toBeActive();
    }

    public void stop() {
        asyncOpThreadPool.shutdownNow();
    }

    public static synchronized Manager getInstance(CuratorFramework client, String serverId,
                                                   DFSManager dfsManager, Filter filter) {
        if (manager == null) {
            manager = new Manager(client, serverId, dfsManager, filter);
        }
        return manager;
    }

    public Status getStatus() {
        return status;
    }

    public String getManagerPath() {
        return managerPath;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    /**
     * 核心方法，manager定期进行manage：
     * 1.刷新任务列表
     * 2.检查Worker
     * 3.发布新的任务
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws ClandRuntimeException.FilterOverflowException bloomFilter的容量已满
     */
    public void manage() throws InterruptedException
            , IOException, ClandRuntimeException.FilterOverflowException {
        if (status == Status.ELECTED) {
            logger.debug("start manage process...");
            checkTasks();
            checkWorkers();
            publishNewTasks();
        }
    }

    /**
     * TODO
     * 接管active职责
     *
     * 平稳地将当前active manager注销，然后
     * 让standby manager接管它的工作
     */
    public void takeOverResponsibility() {
        // 只有standby节点才能接管
        if (status == Status.NOT_ELECTED) {

        }
    }

    /**
     * activeManager的监听器
     *
     * 当其被删除时(失效时)，就开始尝试让
     * 活动的standbyManager(中的某一个)
     * 来接管失效的activeManager
     */
    private Watcher actManagerExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                assert ZNodeStaticSetting.ACTIVE_MANAGER_PATH.equals(watchedEvent.getPath());
                logger.warn("Active manager deleted, now trying to activate manager again. by server."
                        + serverId + " ...");
                recoverActiveManager();
            }
        }
    };

    /**
     * standbyManager的监听器
     *
     * 失效时，尝试重新连接
     */
    private Watcher stdManagerExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            assert managerPath.equals(watchedEvent.getPath());
            if (status == Status.NOT_ELECTED) {
                logger.warn("standby manager deleted, now trying to recover it. by server."
                        + serverId + " ...");
                toBeStandBy();
            }
        }
    };

    /**
     * 集合操作的Callback，尝试恢复activeManager
     *
     * 需要先删除之前自身创建的
     * standby_manager节点，然后
     * 创建active_manager节点。
     * 这2个操作中的任何一个操作失败，
     * 则整个操作失败。
     */
    private BackgroundCallback recoverMultiCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        logger.warn("CONNECTIONLOSS, retrying to recover active manager. server."
                                + serverId + " ...");
                        recoverActiveManager();
                        break;
                    case OK:
                        status = Status.ELECTED;
                        managerPath = ZNodeStaticSetting.ACTIVE_MANAGER_PATH;
                        logger.info("Recover active manager success. now server." + serverId
                                + " is active manager.");
                        activeManagerExists();
                        break;
                    case NODEEXISTS:
                        status = Status.NOT_ELECTED;
                        logger.info("Active manager has already recover by other server.");
                        activeManagerExists();
                        break;
                    default:
                        status = Status.NOT_ELECTED;
                        logger.error("Something went wrong when recoving for active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };


    /*
     * 恢复active_manager
     *
     * status记录了此节点之前是否是active状态，
     * 是则立刻重新获取active权利，否则先等待
     * JITTER_DELAY秒，然后尝试获取active权利
     *
     * 这样做的原因是为了防止网络抖动造成的
     * active_manager被误杀
     */
    @Async
    private void recoverActiveManager() {
        if (status == Status.NOT_ELECTED) {
            status = Status.RECOVERING;
            delayExector.schedule(() -> {
                try {
                    CuratorOp deleteOp = client.transactionOp()
                            .delete()
                            .forPath(managerPath);
                    CuratorOp createOp = client.transactionOp()
                            .create()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
                    client.transaction()
                            .inBackground(recoverMultiCallback, asyncOpThreadPool)
                            .forOperations(deleteOp, createOp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, ZNodeStaticSetting.JITTER_DELAY, TimeUnit.SECONDS);
        } else {
            toBeActive();
        }
    }

    private BackgroundCallback actManagerExistsCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                Stat stat = curatorEvent.getStat();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        activeManagerExists();
                        break;
                    case OK:
                        if (stat == null) {
                            recoverActiveManager();
                            break;
                        }
                        break;
                    default:
                        checkActiveManager();
                        break;
                }
            };

    /**
     * 检查active_manager节点是否还存在
     * 并且设置监听点
     */
    @Async
    private void activeManagerExists() {
        Watcher watcher = status == Status.NOT_ELECTED ?
                actManagerExistsWatcher : null;
        try {
            client.checkExists()
                    .usingWatcher(watcher)
                    .inBackground(actManagerExistsCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("Unknow error.", e);
        }
    }

    private BackgroundCallback stdManagerExistsCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        standbyManagerExists();
                        break;
                    case OK:
                        // pass
                        break;
                    case NONODE:
                        /* 有可能是standy节点转为了active状态，那个时候便不需要重新设置standby节点 */
                        if (status == Status.NOT_ELECTED) {
                            toBeStandBy();
                            logger.warn("standby manager deleted, now trying to recover it. by server."
                                    + serverId + " ...");
                        }
                        break;
                    default:
                        logger.error("Something went wrong when check standby manager itself.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 检查自身standby_manager节点是否还存在
     * 并且设置监听点
     */
    @Async
    private void standbyManagerExists() {
        try {
            client.checkExists()
                    .usingWatcher(stdManagerExistsWatcher)
                    .inBackground(stdManagerExistsCallback, asyncOpThreadPool)
                    .forPath(managerPath);
        } catch (Exception e) {
            logger.warn("Unknow error.", e);
        }
    }

    private BackgroundCallback actManagerCreateCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkActiveManager();
                        break;
                    case OK:
                        logger.info("Active manager created success. at {}", new Date().toString());
                        managerPath = path;
                        status = Status.ELECTED;
                        activeManagerExists();
                        break;
                    case NODEEXISTS:
                        logger.info("Active manger already exists, turn to set standby manager...");
                        toBeStandBy();
                        break;
                    default:
                        logger.error("Something went wrong when running for active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 激活active_manager
     */
    @Async
    private void toBeActive() {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground(actManagerCreateCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }

    private BackgroundCallback stdManagerCreateCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        toBeStandBy();
                        break;
                    case OK:
                        status = Status.NOT_ELECTED;
                        managerPath = path;
                        logger.info("Server." + serverId + " registered. at {}", new Date().toString());
                        activeManagerExists();
                        standbyManagerExists();
                        break;
                    case NODEEXISTS:
                        //TODO
                        break;
                    default:
                        logger.error("Something went wrong when running for stand manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 激活standby_manager
     */
    @Async
    private void toBeStandBy() {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground(stdManagerCreateCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.STANDBY_MANAGER_PATH + serverId);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }

    private BackgroundCallback actCheckCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkActiveManager();
                        break;
                    case NONODE:
                        recoverActiveManager();
                        break;
                    default:
                        logger.error("Something went wrong when check active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 检查active_manager的状态
     */
    @Async
    private void checkActiveManager() {
        try {
            client.getData()
                    .inBackground(actCheckCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }


    /**
     * 检查已经完成的任务，把对应的
     * 存放url的文件移出等待队列
     */
    private void checkTasks() throws InterruptedException, IOException {
        syncWaitingTasks();
        /* 更新tasksInfo状态表 */
        taskManager.checkTasks();
        Map<String, Epoch> tasks = taskManager.getTasksInfo();
        Iterator<Entry<String, Epoch>> iterator = tasks.entrySet().iterator();
        while (iterator.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            Epoch value = (Epoch) entry.getValue();
            switch (value.getStatus()) {
                case FINISHED:
                    if (unfinishedTaskMap.containsKey(key)) {
                        unfinishedTaskMap.remove(key);
                    }
                    dfsManager.move(configuration.WAITING_TASKS_URLS + "/" + key,
                            configuration.FINISHED_TASKS_URLS + "/" + key);
                    taskManager.asyncReleaseTask(ZNodeStaticSetting.TASKS_PATH + '/' + key);
                    break;
                case RUNNING:
                    unfinishedTaskMap.put(key, value);
                    break;
                case WAITING:
                    unfinishedTaskMap.remove(key);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 用于防止用户误将znode下task删除导致任务永久失效的情况
     *
     * 保证hdfs中waiting tasks与znode中cdTasks一致
     * 但是无法保证znode中cdTasks与hdfs中waitingtasks中一致
     */
    private void syncWaitingTasks() throws IOException {
        dfsManager.listFiles(configuration.WAITING_TASKS_URLS, false)
                .stream()
                .map(Files::getNameWithoutExtension)
                .forEach(fileName -> taskManager.asyncSubmit(fileName));
    }

    /**
     * 检查Workers的状态，若它失效则需要重置它之前领取的任务。
     */
    private void checkWorkers() throws InterruptedException {
        workersWatcher.refreshAliveWorkers();
        workersWatcher.refreshAllWorkersStatus();
        workersMap = workersWatcher.getWorkersMap();
        unfinishedTaskMap.entrySet()
                .stream()
                .filter(entry -> {
                    String name = entry.getKey();
                    Epoch epoch = entry.getValue();
                    return !workersMap.containsKey(name) && epoch.getDifference() > configuration.WORKER_DEAD_TIME;
                })
                .forEach(entry -> {
                    String name = entry.getKey();
                    taskManager.asyncResetTask(ZNodeStaticSetting.TASKS_PATH + "/" + name);
                    logger.warn("The owner of task: " + name + " has dead, now reset it...");
                });
    }

    /**
     * 发布新的任务
     */
    private void publishNewTasks() throws IOException, ClandRuntimeException.FilterOverflowException {
        String tempSavePath = configuration.BLOOM_TEMP_DIR;
        List<String> hdfsUrlFiles = dfsManager.listFiles(
                configuration.NEW_TASKS_URLS, false);
        if (hdfsUrlFiles.size() == 0) {
            /* 没有需要处理的新URL文件 */
            return;
        }


        /*
            TODO
            目前是先将所有要处理的文件下载下来再进行处理
            Q:为什么不开多个线程进行处理呢，这样不是能先下载的文件先进行处理，不被IO阻塞吗？
            A:原因是需要对文件进行指定大小的切片，多线程的情况下切片比较难处理，还需要一些预处理，未来会将这里改为多线程
         */
        List<String> urlFiles = downloadTaskFiles(hdfsUrlFiles, tempSavePath);

        /*
            后续整体工作流程：
            对每个文件进行逐个按行读取，在开始读的同时也会
            在本地新建一个同名的.bak文件，每读一行后会尝试将
            其录入过滤器，若成功则说明此url是新的url，会将
            其写入.bak文件中。完毕后会删除其他非.bak的文件
            ，再去掉.bak文件的.bak后缀。
            然后将处理后的所有文件上传到hdfs上，确保上传成
            功后才会到znode中发布任务
        */
        filterUrlAndSave(urlFiles, tempSavePath);
        File file = new File(tempSavePath);
        deleteNormalFiles(file);
        removeTempSuffix(file);
        submitNewTasks(file);

        /*
            TODO：
            当文件很大时会占用大量IO
            需要另外一种方式来备份
            目前想参照fsimage-edits的模式
            这个需要阅读其实现源码
        */

        /* 备份 */
        backUpFilterCache();

        /*
            在hdfs上删除处理
            完毕的的new url文件
         */
        for (String urlPath : hdfsUrlFiles) {
            dfsManager.delete(urlPath, false);
        }
    }

    /**
     * 删除多余文件(不以Configuration.TEMP_SUFFIX结尾的文件)
     *
     * @param tempSaveDir
     */
    private void deleteNormalFiles(File tempSaveDir) {
        Optional.ofNullable(tempSaveDir.listFiles()).ifPresent(files ->
                Arrays.stream(files)
                        .filter(File::isFile)
                        .filter(file -> !file.getAbsolutePath().endsWith(configuration.TEMP_SUFFIX))
                        .forEach(File::delete)
        );
    }

    /**
     * 去除文件的Configuration.TEMP_SUFFIX后缀
     *
     * @param dir
     */
    private void removeTempSuffix(File dir) {
        Optional.ofNullable(dir.listFiles()).ifPresent(files ->
                Arrays.stream(files)
                        .filter(File::isFile)
                        .filter(file -> file.getAbsolutePath().endsWith(configuration.TEMP_SUFFIX))
                        .forEach(file -> {
                            String path = file.getAbsolutePath();
                            file.renameTo(new File(path.substring(0,
                                    path.length() - configuration.TEMP_SUFFIX.length())));
                        })
        );
    }

    /**
     * 上传新任务到HDFS，然后发布任务到ZooKeeper中
     *
     * @param dir
     * @throws IOException
     */
    private void submitNewTasks(File dir) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (!file.isDirectory()) {
                String filePath = file.getAbsolutePath();
                /*
                    若文件已存在则直接跳过
                    可以这么做的原因是文件名是根据内容生成的md5码
                    相同则基本可以确定就是同一个文件，没必要重复上传
                */
                if (!dfsManager.exist(filePath)) {
                    dfsManager.uploadFile(filePath,
                            configuration.WAITING_TASKS_URLS + '/' + file.getName());
                }
                taskManager.asyncSubmit(file.getName());
            }
        }
    }

    /**
     * 从hdfs下新任务文件下载到本地
     *
     * @param urlFiles hdfs中的文件路径
     * @param savePath 保存路径
     * @return
     * @throws IOException
     */
    private List<String> downloadTaskFiles(List<String> urlFiles
            , String savePath) throws IOException {
        List<String> localUrlFiles = new LinkedList<>();
        for (String filePath : urlFiles) {
            /*
                若文件已存在则直接跳过
                可以这么做的原因是文件名是根据内容生成的md5码
                相同则基本可以确定就是同一个文件，没必要重复下载
            */
            File temp = new File(filePath);
            File localFile = new File(savePath
                    + File.separator + temp.getName());
            if (!localFile.exists()) {
                dfsManager.downloadFile(filePath, savePath);
            }
            localUrlFiles.add(localFile.getAbsolutePath());
        }
        return localUrlFiles;
    }

    /**
     * 备份filter的缓存文件到hdfs
     * 注意：目前而言，会删除原来的旧缓存文件（无论是本地还是hdfs中）
     *
     * @throws IOException
     */
    public void backUpFilterCache() throws IOException {
        /* 备份之前删除原来的缓存文件 */
        File localSave = new File(configuration.BLOOM_SAVE_PATH);
        Optional.ofNullable(localSave.listFiles()).ifPresent(files ->
                Arrays.stream(files)
                        .filter(File::isFile)
                        .forEach(File::delete)
        );
        /* 备份至本地 */
        String bloomFilePath = filter.save(configuration.BLOOM_SAVE_PATH);
        /* 上传至dfs */
        dfsManager.uploadFile(bloomFilePath, configuration.BLOOM_BACKUP_PATH);

        /* 删除dfs上旧的缓存文件，去除新缓存文件的TEMP_SUFFIX后缀 */
        List<String> cacheFiles
                = dfsManager.listFiles(configuration.BLOOM_BACKUP_PATH, false);
        for (String cache : cacheFiles) {
            if (!cache.endsWith(configuration.TEMP_SUFFIX)) {
                dfsManager.delete(cache, false);
            } else {
                String newName = cache.substring(0,
                        cache.length() - configuration.TEMP_SUFFIX.length());
                dfsManager.move(cache, newName);
            }
        }

    }

    /**
     * 遍历下载下来的保存着url的文件
     * 以行为单位将其放入过滤器
     * 过滤后的url会被以固定的数量
     * 切分为若干个文件
     *
     * @param urlFiles
     * @throws IOException
     */
    private void filterUrlAndSave(List<String> urlFiles
            , final String saveDir) throws IOException {

        /* 用AtomicLong的原因只是为了能在匿名类中计数 */
        final AtomicLong newUrlCounter = new AtomicLong(0);
        final StringBuilder newUrls = new StringBuilder();
        final MD5Maker md5 = new MD5Maker();
        for (String filePath : urlFiles) {
            File file = new File(filePath);
            /* 每读一定数量的URLS就将其写入新的文件 */
            Files.readLines(file,
                    Charset.defaultCharset(), new LineProcessor<Object>() {
                        @Override
                        public boolean processLine(String line) throws IOException {
                            String newLine = line + System.getProperty("line.separator");
                            /* 到filter中确认url是不是已经存在，已经存在就丢弃 */
                            if (filter.put(line)) {
                                md5.update(newLine);
                                if (newUrlCounter.get() <= configuration.TASK_URLS_NUM) {
                                    newUrls.append(newLine);
                                    newUrlCounter.incrementAndGet();
                                } else {
                                    /* 文件名是根据其内容生成的md5值 */
                                    String urlFileName = saveDir + File.separator
                                            + md5.toString()
                                            + configuration.TEMP_SUFFIX;
                                    Files.write(newUrls.toString().getBytes()
                                            , new File(urlFileName));
                                    newUrls.delete(0, newUrls.length());
                                    newUrlCounter.set(0);
                                    md5.reset();
                                }
                            }
                            return true;
                        }

                        @Override
                        public Object getResult() {
                            /* 处理残留的urls */
                            if (newUrls.length() > 0) {
                                String urlFileName = saveDir + File.separator
                                        + md5.toString()
                                        + configuration.TEMP_SUFFIX;
                                try {
                                    Files.write(newUrls.toString().getBytes()
                                            , new File(urlFileName));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                newUrls.delete(0, newUrls.length());
                                newUrlCounter.set(0);
                            }
                            return null;
                        }
                    });
        }
    }
}
