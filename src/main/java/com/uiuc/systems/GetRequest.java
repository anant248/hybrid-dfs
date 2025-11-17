package com.uiuc.systems;
import java.io.Serializable;

public class GetRequest implements Serializable {
    private String fileName;

    public GetRequest(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}