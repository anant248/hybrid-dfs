package com.uiuc.systems;

import java.io.Serializable;

public class TaskLogMessage implements Serializable {
    private final int taskId;
    private final String logLine;

    public TaskLogMessage(int taskId, String logLine) {
        this.taskId = taskId;
        this.logLine = logLine;
    }

    public int getTaskId() { return taskId; }
    public String getLogLine() { return logLine; }
}

