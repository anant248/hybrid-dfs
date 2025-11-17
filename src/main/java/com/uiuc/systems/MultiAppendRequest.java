package com.uiuc.systems;

import java.io.Serializable;

public class MultiAppendRequest implements Serializable {
    private String localFileName;
    private String hdfsFileName;

    public MultiAppendRequest(String localFileName, String hdfsFileName) {
        this.localFileName = localFileName;
        this.hdfsFileName = hdfsFileName;
    }

    public String getLocalFileName() {
        return localFileName;
    }

    public void setLocalFileName(String localFileName) {
        this.localFileName = localFileName;
    }

    public String getHdfsFileName() {
        return hdfsFileName;
    }

    public void setHdfsFileName(String hdfsFileName) {
        this.hdfsFileName = hdfsFileName;
    }
}
