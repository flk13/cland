package com.rynuk.cland.zk.worker;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.exception.ClandRuntimeException;
import com.rynuk.cland.zk.task.Epoch;
import com.rynuk.cland.zk.task.TaskData;
import com.rynuk.cland.zk.task.TaskWatcher;
import com.rynuk.cland.zk.task.TaskWorker;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private CuratorFramework client;

    private String serverId;

    private String workerPath;

    private TaskWorker taskWorker;

    private TaskWatcher taskWatcher;

    public Worker(CuratorFramework client, String serverId) {
        this.client = client;
        this.serverId = serverId;
        taskWorker = new TaskWorker(client);
        taskWatcher = new TaskWatcher(client);
        signUpWorker();
    }

    public static void addToBlackList(String taskName) {
        TaskWorker.addToBlackList(taskName);
    }

    public static void clearBlackList() {
        TaskWorker.clearTaskBlackList();
    }

    public TaskWorker getTaskWorker() {
        return taskWorker;
    }

    public TaskWatcher getTaskWatcher() {
        return taskWatcher;
    }

    public String getWorkerPath() {
        return workerPath;
    }

    public void waitForTask() {
        taskWatcher.waitForTask();
    }

    /**
     * 设置Worker的工作状态
     *
     * @param taskName 正在执行任务的ID，为空则说明当前没有任务
     */
    public void setStatus(String taskName) {
        try {
            client.setData().forPath(workerPath, taskName.getBytes());
        } catch (Exception e) {
            logger.warn("failed to set task.", e);
        }
    }

    public Epoch takeTask() {
        Optional<Epoch> task = Optional.ofNullable(taskWorker.takeTask());
        task.ifPresent((val) -> setStatus(val.getTaskName()));
        return task.get();
    }

    public boolean beat(String taskName, TaskData taskData) {
        boolean res = taskWorker.setRunningTask(ZNodeStaticSetting.TASKS_PATH + '/' + taskName, -1, taskData);
        setStatus(taskName);
        return res;
    }

    public boolean discardTask(String taskPath) {
        return taskWorker.discardTask(taskPath);
    }

    public boolean finishTask(String taskPath) {
        boolean res = taskWorker.finishTask(taskPath);
        setStatus("");
        return res;
    }

    private void signUpWorker() {
        try {
            workerPath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(ZNodeStaticSetting.NEW_WORKER_PATH + serverId, serverId.getBytes());
        } catch (KeeperException.ConnectionLossException e) {
            signUpWorker();
        } catch (Exception e) {
            throw new ClandRuntimeException.OperationFailedException("\nfailed to sign up worker. " + e.getMessage());
        }
    }
}
