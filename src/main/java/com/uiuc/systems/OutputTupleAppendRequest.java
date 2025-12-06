package com.uiuc.systems;

import java.io.Serializable;

public class OutputTupleAppendRequest implements Serializable {
    private byte[] data;
    private String fileName;
    private int appendIndex;

    public OutputTupleAppendRequest(byte[] data, String fileName) {
        this.data = data;
        this.fileName = fileName;
    }

    public OutputTupleAppendRequest(byte[] data, String fileName, int appendIndex) {
        this.data = data;
        this.fileName = fileName;
        this.appendIndex = appendIndex;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getAppendIndex() {
        return appendIndex;
    }

    public void setAppendIndex(int appendIndex) {
        this.appendIndex = appendIndex;
    }
}