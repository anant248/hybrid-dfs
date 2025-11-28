package com.uiuc.systems;

public class DownstreamTarget {
    public String host;
    public int port;
    public int taskId;

    public DownstreamTarget(String host, int port, int taskId) {
        this.host = host;
        this.port = port;
        this.taskId = taskId;
    }

    // getters and setters

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getTaskId() {
        return taskId;
    }
}
