package com.uiuc.systems;

public class GlobalHyDFS {
    public static HyDFS hdfs;

    public GlobalHyDFS() { }

    public static HyDFS getHdfs() {
        return hdfs;
    }

    public void setHdfs(HyDFS hdfs) {
        GlobalHyDFS.hdfs = hdfs;
    }
}
