package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class WorkerLogBatch implements Serializable {
    private int taskId;
    private List<String> lines;

    public WorkerLogBatch(int taskId, List<String> lines) {
        this.taskId = taskId;
        this.lines = lines;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public List<String> getLines() {
        return lines;
    }

    public void setLines(List<String> lines) {
        this.lines = lines;
    }
}
