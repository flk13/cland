package com.rynuk.cland.zk.task;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class TaskWorker extends Task {
    /* 任务黑名单 */
    private static Set<String> blackList = new ConcurrentSkipListSet<>();

    public TaskWorker(CuratorFramework client) {
        super(client);
    }

    /**
     * 接管任务
     *
     * Q:这里可能会有一个疑问，那就是为何不使用sync
     * A:就目前而言，还找不到使用它的理由，因为强实时性的意义并不大，
     * 即使本地zookeeper的视图稍有落后，也并不会发生多个worker持有一个任务的情况发生（会验证Task的版本信息）
     * 只是会多一些抢夺次数，而频繁的sync可能会给服务器带来更大的负担
     */
    public Epoch takeTask() {
        Epoch task = null;
        checkTasks();
        /* 抢夺未被领取的任务 */
        Iterator<Entry<String, Epoch>> iterator = super.tasksInfo.entrySet().iterator();
        while (iterator.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            Epoch value = (Epoch) entry.getValue();
            if (!blackList.contains(value) && value.getStatus() == Status.WAITING) {
                if (setRunningTask(key,
                        value.getDataVersion(), value.getTaskData())) {
                    task = value;
                    break;
                }
            }
        }
        /* 如果task不为null就说明拿到了任务 */
        return task;
    }

    public static void clearTaskBlackList() {
        blackList.clear();
    }

    public static void removeTaskBlackListElement(String taskName) {
        blackList.remove(taskName);
    }

    public static void addToBlackList(String taskName) {
        blackList.add(taskName);
    }

    /**
     * 执行失败，放弃任务
     *
     * @param taskPath
     */
    public boolean discardTask(String taskPath) {
        boolean res = false;
        try {
            TaskData taskData = new TaskData();
            taskData.setStatus(Status.WAITING);
            client.setData().forPath(taskPath, taskData.getBytes());
            res = true;
        } catch (KeeperException.ConnectionLossException e) {
            discardTask(taskPath);
        } catch (Exception e) {
            logger.warn("discard task" + taskPath + " failed", e);
        }
        return res;
    }

    /**
     * 完成任务
     *
     * @param taskPath
     */
    public boolean finishTask(String taskPath) {
        boolean res = false;
        try {
            byte[] data = client.getData().forPath(taskPath);
            TaskData taskData = new TaskData(data);
            taskData.setStatus(Status.FINISHED);
            client.setData().forPath(taskPath, taskData.getBytes());
            res = true;
        } catch (KeeperException.ConnectionLossException e) {
            finishTask(taskPath);
        } catch (Exception e) {
            logger.error("set task" + taskPath + " finished failed", e);
        }
        return res;
    }


    /**
     * 刷新本地zk视图
     *
     * @return
     */
    public boolean sync() {
        boolean res = true;
        try {
            client.sync().forPath(ZNodeStaticSetting.ROOT_PATH);
        } catch (Exception e) {
            res = false;
            logger.error("synchronized local view failed", e);
        }
        return res;
    }

    /**
     * 尝试将一个task节点置为running状态
     * 若成功即拿到了该任务
     * 也可作为心跳信息（改变了mtime）
     *
     * @param path
     * @param version
     * @return
     */
    public boolean setRunningTask(String path, int version, TaskData data) {
        boolean res = false;
        TaskData taskData = new TaskData(data.getBytes());
        taskData.setStatus(Status.RUNNING);
        try {
            client.setData().withVersion(version).forPath(path, taskData.getBytes());
            res = true;
        } catch (KeeperException.NoNodeException e) {
            super.tasksInfo.remove(path);
        } catch (KeeperException.ConnectionLossException e) {
            setRunningTask(path, version, data);
        } catch (Exception e) {
            logger.warn("set running task failed.", e);
        }
        return res;
    }
}
