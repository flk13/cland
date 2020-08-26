package com.rynuk.cland.utils;

/**
 * 定义四种状态：等待、结束、成功、失败
 * @author rynuk
 * @date 2020/7/27
 */
public class Tracker {
    public static final int WAITING = 3;

    public static final int FINNISHED = 1;

    public static final int SUCCESS = 0;

    public static final int FAILED = 2;

    private int status;

    public Tracker() {
        status = WAITING;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void andStatus(int status) {

    }

    public int getStatus() {
        return status;
    }

    public String toString() {
        String result = "";
        switch (status) {
            case WAITING:
                result = "WAITING";
                break;
            case FAILED:
                result = "FAILED";
                break;
            case FINNISHED:
                result = "FINNISHED";
                break;
            case SUCCESS:
                result = "SUCCESS";
                break;
        }
        return result;
    }
}
