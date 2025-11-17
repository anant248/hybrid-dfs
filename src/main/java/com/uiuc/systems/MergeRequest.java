package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class MergeRequest implements Serializable {
    private String fileName;
    private List<NodeId> replicas;

    public MergeRequest(String fileName,List<NodeId> replicas) {
        this.fileName = fileName;
        this.replicas = replicas;
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
