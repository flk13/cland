package com.rynuk.cland.service.rpc;

import com.sun.istack.Nullable;
import com.rynuk.cland.zk.task.Epoch;
import com.rynuk.cland.zk.task.Task;
import com.rynuk.cland.zk.task.TaskData;
import com.rynuk.cland.zk.worker.Worker;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class WorkerCrawlerServiceImpl implements LocalWorkerCrawlerServiceBase.Iface {
    private Worker worker;

    private static final Logger logger = LoggerFactory.getLogger(WorkerCrawlerServiceImpl.class);

    public WorkerCrawlerServiceImpl(Worker worker) {
        this.worker = worker;
    }

    @Override
    public boolean giveUpTask(String taskId, @Nullable String reason) throws TException {
        boolean res = worker.discardTask(taskId);
        /*TODO*/
        Optional.ofNullable(reason).ifPresent(System.out::println);
        return res;
    }

    @Override
    public int getLastProgressRate(String taskId) throws TException {
        return worker.getTaskWorker()
                .checkTask(taskId)
                .getTaskData()
                .getProgress();
    }

    @Override
    public boolean finishTask(String taskId) throws TException {
        return worker.finishTask(taskId);
    }

    //TODO new para: task num, max timeout
    @Override
    public List<String> getNewTask() throws TException {
        Epoch task;
        List<String> res = new ArrayList<>();
        while (true) {
            logger.info("Waiting for task...");
            worker.waitForTask();
            logger.info("Trying to get a task...");
            task = worker.takeTask();
            if (task != null) {
                logger.info("Get task: " + task + " crawler start working...");
                res.add(task.toJson());
                break;
            }
        }
        return res;
    }

    @Override
    public List<String> getBlackList() throws TException {
        return null;
    }

    @Override
    public boolean addToBlackList(String taskId) throws TException {
        return false;
    }

    @Override
    public boolean removeFromBlackList(String taskId) throws TException {
        return false;
    }

    @Override
    public boolean clearBlackList() throws TException {
        return false;
    }

    @Override
    public boolean updateTaskProgressRate(String taskId, int newProgressRate, int markup, byte status) throws TException {
        TaskData taskData = new TaskData();
        taskData.setProgress(newProgressRate)
                .setUniqueMarkup(markup)
                .setStatus(Task.Status.get(Byte.toString(status)));
        return worker.beat(taskId, taskData);
    }

    @Override
    public String getWorkersStatus() throws TException {
        return null;
    }

    @Override
    public String getFiltersStatus() throws TException {
        return null;
    }

    @Override
    public String getManagersStatus() throws TException {
        return null;
    }
}
