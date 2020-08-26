package com.rynuk.cland.zk.task;

import com.rynuk.cland.utils.Bytes;

/**
 * HIGH                    --->                   LOW
 * -----------------------------------------------------------
 * 1*byte     |          4*byte      |           4*byte
 * 状态码      |      bloom标识符预留   |   完成度（处理完毕url的条数）
 * -----------------------------------------------------------
 *
 * @author rynuk
 * @date 2020/8/11
 */
public class TaskData {
    private byte[] data;

    private int uniqueMarkup;

    private int progress;

    private byte status;

    private static short STATUS = 8;

    private static short U_MARKUP = 4;

    private static short PROGRESS = 0;

    public TaskData() {
        data = new byte[9];
    }

    public TaskData(byte[] data) {
        if (data.length != 9) {
            throw new IllegalArgumentException("Illegal data value");
        }
        this.data = data;
        status = data[STATUS];
        progress = Bytes.bytesToInt(new byte[]{data[PROGRESS], data[PROGRESS + 1], data[PROGRESS + 2], data[PROGRESS + 3]});
        uniqueMarkup = Bytes.bytesToInt(new byte[]{data[U_MARKUP], data[U_MARKUP + 1], data[U_MARKUP + 2], data[U_MARKUP + 3]});
    }

    public TaskData setStatus(Task.Status status) {
        byte value = Byte.parseByte(status.getValue());
        this.status = value;
        data[STATUS] = value;
        return this;
    }

    public TaskData setProgress(int progress) {
        this.progress = progress;
        byte[] bytes = Bytes.intToBytes(progress);
        for (int i = 0; i < 4; ++i) {
            data[i + PROGRESS] = bytes[i];
        }
        return this;
    }

    public TaskData setUniqueMarkup(int uniqueMarkup) {
        this.uniqueMarkup = uniqueMarkup;
        byte[] bytes = Bytes.intToBytes(uniqueMarkup);
        for (int i = 0; i < 4; ++i) {
            data[i + U_MARKUP] = bytes[i];
        }
        return this;
    }

    public int getProgress() {
        return progress;
    }

    public int getUniqueMarkup() {
        return uniqueMarkup;
    }

    public Task.Status getStatus() {
        return Task.Status.get(Byte.toString(status));
    }

    public byte[] getBytes() {
        return data;
    }

    @Override
    public String toString() {
        return "[status: " + getStatus()
                + " | uniqueMarkup: " + uniqueMarkup
                + " | progress: " + progress + ']';
    }
}
