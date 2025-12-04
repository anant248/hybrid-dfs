package com.uiuc.systems;

import java.io.Serializable;

public class GetPidRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int taskId;

    public GetPidRequest(int taskId) { this.taskId = taskId; }
    
    public int getTaskId() { return taskId; }
}