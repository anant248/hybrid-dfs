package com.uiuc.systems;

import java.io.Serializable;

public class WorkerTaskFailRequest implements Serializable {
    private int currentTaskId;
    private int failedTaskId;

    public WorkerTaskFailRequest(int currentTaskId, int failedTaskId) {
        this.currentTaskId = currentTaskId;
        this.failedTaskId = failedTaskId;
    }

    public int getCurrentTaskId() {
        return currentTaskId;
    }

    public void setCurrentTaskId(int currentTaskId) {
        this.currentTaskId = currentTaskId;
    }

    public int getFailedTaskId() {
        return failedTaskId;
    }

    public void setFailedTaskId(int failedTaskId) {
        this.failedTaskId = failedTaskId;
    }
}
