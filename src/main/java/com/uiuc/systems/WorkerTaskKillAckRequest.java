package com.uiuc.systems;

import java.io.Serializable;

public class WorkerTaskKillAckRequest implements Serializable {
    private int taskId;
    private boolean killed;

    public WorkerTaskKillAckRequest(int taskId, boolean killed) {
        this.taskId = taskId;
        this.killed = killed;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public boolean isKilled() {
        return killed;
    }

    public void setKilled(boolean killed) {
        this.killed = killed;
    }
}
