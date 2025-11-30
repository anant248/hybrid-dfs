package com.uiuc.systems;

public class GlobalHyDFS {
    public static HyDFS hdfs;

    public GlobalHyDFS(HyDFS hdfs) {
        this.hdfs = hdfs;
    }

    public HyDFS getHdfs() {
        return hdfs;
    }

    public void setHdfs(HyDFS hdfs) {
        this.hdfs = hdfs;
    }
}
