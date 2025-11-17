package com.uiuc.systems;

public class MergeReplicaRequest {
    private byte[] fileData;
    private String fileName;
    private int index;

    public MergeReplicaRequest(byte[] fileData, String fileName,int index) {
        this.fileData = fileData;
        this.fileName = fileName;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
