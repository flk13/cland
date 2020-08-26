package com.rynuk.cland.api.jsondata;

/**
 * @author rynuk
 * @date 2020/7/28
 */
public class WorkerJson implements JData{
    /* 名称 */
    private String name;

    /* 当前执行的任务 */
    private String currentTask;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCurrentTask() {
        return currentTask;
    }

    public void setCurrentTask(String currentTask) {
        this.currentTask = currentTask;
    }
}
