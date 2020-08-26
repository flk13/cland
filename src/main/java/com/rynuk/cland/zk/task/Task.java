package com.rynuk.cland.zk.task;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class Task {
    public enum Status {
        WAITING("0"), RUNNING("1"), FINISHED("2");
        private final String value;

        Status(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Status get(String type) {
            switch (type) {
                case "0":
                    return WAITING;
                case "1":
                    return RUNNING;
                case "2":
                    return FINISHED;
                default:
                    return null;
            }
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(Task.class);

    protected CuratorFramework client;

    protected Map<String, Epoch> tasksInfo = new ConcurrentHashMap<>();

    public Task(CuratorFramework client) {
        this.client = client;
    }

    /**
     * 遍历目前所有任务
     *
     * 成功后对每个task进行checkTask
     */
    public void checkTasks() {
        try {
            client.getChildren()
                    .forPath(ZNodeStaticSetting.TASKS_PATH)
                    .forEach((task) -> {
                        String taskPath = ZNodeStaticSetting.NEW_TASK_PATH + task;
                        Optional<Epoch> info = Optional.ofNullable(checkTask(taskPath));
                        info.ifPresent(val -> tasksInfo.put(taskPath, val));
                    });
        } catch (Exception e) {
            logger.warn("failed to update tasks' information", e);
        }
    }

    /**
     * 检查任务
     *
     * 获取任务的信息
     *
     * @param path
     */
    public Epoch checkTask(String path) {
        Epoch res = null;
        try {
            String taskName = new File(path).getName();
            Stat stat = new Stat();
            byte[] data = client.getData()
                    .storingStatIn(stat)
                    .forPath(path);
            TaskData taskData = new TaskData(data);
            res = new Epoch(taskName, stat.getMtime(), stat.getVersion(), taskData);
        } catch (Exception e) {
            logger.warn("Check task: " + path + " failed.", e);
        }
        return res;
    }


    /**
     * 获取所有未完成Task的信息
     * Epoch中包含
     * - 上一次修改的时间
     * - 最后检查的时间
     * - 状态
     * 信息新鲜度取决于上一次checkTasks的时间
     *
     * @return
     */
    public Map<String, Epoch> getTasksInfo() {
        return new HashMap<>(tasksInfo);
    }
}
