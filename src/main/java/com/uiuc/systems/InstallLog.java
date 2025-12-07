package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class InstallLog implements Serializable {
    public final int taskId;
    public final List<String> lines;
    public InstallLog(int taskId, List<String> lines) {
        this.taskId = taskId;
        this.lines = lines;
    }
}

