package com.rynuk.cland.api.jsondata;

import com.rynuk.cland.zk.task.Task;

import java.util.Date;

/**
 * @author rynuk
 * @date 2020/7/28
 */
public class TaskJson implements JData{
    /* 创建时间 */
    private long cTime;

    /* 最后一次修改时间 */
    private long mTime;

    /* 路径 */
    private String name;

    /* 状态 */
    private Task.Status status;

    /* 任务进度 */
    private int progress;

    /* 所属filter标识 */
    private int markup;

    public String getCtime() {
        return new Date(cTime).toString();
    }

    public void setCtime(long cTime) {
        this.cTime = cTime;
    }

    public String getMtime() {
        return new Date(mTime).toString();
    }

    public void setMtime(long mTime) {
        this.mTime = mTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        String result = "Unknow";
        switch (status) {
            case WAITING:
                result = "WAITING";
                break;
            case RUNNING:
                result = "RUNNING";
                break;
            case FINISHED:
                result = "FINISHED";
                break;
            default:
                break;
        }
        return result;
    }

    public void setStatus(Task.Status status) {
        this.status = status;
    }

    public String getProgress() {
        return new Integer(progress).toString();
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getMarkup() {
        return new Integer(markup).toString();
    }

    public void setMarkup(int markup) {
        this.markup = markup;
    }
}
