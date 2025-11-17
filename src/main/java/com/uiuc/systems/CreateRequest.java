package com.uiuc.systems;

import java.io.Serializable;

public class CreateRequest implements Serializable {
    private byte[] fileData;
    private String fileName;

    public CreateRequest(byte[] fileData, String fileName) {
        this.fileData = fileData;
        this.fileName = fileName;
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
