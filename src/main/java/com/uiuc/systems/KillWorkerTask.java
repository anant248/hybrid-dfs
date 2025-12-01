package com.uiuc.systems;

import java.io.Serializable;

public class KillWorkerTask implements Serializable {
    private int taskId;

    public KillWorkerTask(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }
}
