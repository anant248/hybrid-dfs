package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class ReReplicaRequest implements Serializable {
    private String fileName;
    private byte[] fileData;
    private List<NodeId> replicas;
    private int lastIndex;

    public ReReplicaRequest(String fileName, byte[] fileData, List<NodeId> replicas, int lastIndex) {
        this.fileName = fileName;
        this.fileData = fileData;
        this.replicas = replicas;
        this.lastIndex = lastIndex;
    }

    public String getFileName() { return fileName; }
    public byte[] getFileData() { return fileData; }
    public List<NodeId> getReplicas() { return replicas; }
    public int getLastIndex() { return lastIndex; }
}
