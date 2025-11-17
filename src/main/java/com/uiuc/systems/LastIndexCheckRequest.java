package com.uiuc.systems;

import java.io.Serializable;

public class LastIndexCheckRequest implements Serializable{
    private String fileName;

    public LastIndexCheckRequest(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
