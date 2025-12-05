package com.uiuc.systems;

import java.io.Serializable;

public class FinalTuple implements Serializable {
    private String line;
    private int taskId;

    public FinalTuple(int taskId, String line) {
        this.line = line;
        this.taskId = taskId;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }
}
