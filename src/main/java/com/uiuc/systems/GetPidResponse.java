package com.uiuc.systems;

import java.io.Serializable;

public class GetPidResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int taskId;
    private final long pid; // -1 if not found

    public GetPidResponse(int taskId, long pid) { this.taskId = taskId; this.pid = pid; }

    public int getTaskId() { return taskId; }
    
    public long getPid() { return pid; }
}