package com.uiuc.systems;

import java.util.List;

public class FileMetaData {
    private NodeId owner;
    private List<NodeId> replicas;
    private String fileName;
    private int lastIndex;

    public FileMetaData(NodeId owner, List<NodeId> replicas, String fileName, int lastIndex) {
        this.owner = owner;
        this.replicas = replicas;
        this.fileName = fileName;
        this.lastIndex = lastIndex;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public synchronized int nextLastIndex() {
        return this.lastIndex + 1;
    }

    public synchronized  void setLastIndex(int lastIndex) {
        if(lastIndex > this.lastIndex)
        {
            this.lastIndex = lastIndex;
        }
    }

    public NodeId getOwner() {
        return owner;
    }

    public void setOwner(NodeId owner) {
        this.owner = owner;
    }

    public List<NodeId> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<NodeId> replicas) {
        this.replicas = replicas;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
