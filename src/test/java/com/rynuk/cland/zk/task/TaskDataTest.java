package com.rynuk.cland.zk.task;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public class TaskDataTest {
    @Test
    public void parseTest() {
        TaskData data = new TaskData();
        data.setStatus(Task.Status.WAITING).setProgress(300).setUniqueMarkup(200);
        TaskData data1 = new TaskData(data.getBytes());
        assertEquals(data1.toString(), data.toString());
    }
}
