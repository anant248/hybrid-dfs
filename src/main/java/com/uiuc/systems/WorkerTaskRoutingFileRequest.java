package com.uiuc.systems;

import java.io.Serializable;

public class WorkerTaskRoutingFileRequest implements Serializable {
    private String fileContent;
    private int taskId;

    public WorkerTaskRoutingFileRequest(String fileContent, int taskId) {
        this.fileContent = fileContent;
        this.taskId = taskId;
    }

    public String getFileContent() {
        return fileContent;
    }

    public void setFileContent(String fileContent) {
        this.fileContent = fileContent;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }
}
