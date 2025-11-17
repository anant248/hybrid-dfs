package com.uiuc.systems;
import java.io.Serializable;

public class FileCheckRequest implements Serializable {
    private String fileName;

    public FileCheckRequest(String fileName) { 
        this.fileName = fileName; 
    }

    public String getFileName() { 
        return fileName; 
    }
}
