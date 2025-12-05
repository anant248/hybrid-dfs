package com.uiuc.systems;

import java.io.Serializable;

public class LoadStateRequest implements Serializable {
    private final int taskId;
    public LoadStateRequest(int taskId) {
        this.taskId = taskId;
    }
    public int getTaskId() {
        return taskId;
    }
}
