package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class FetchLogResponse implements Serializable {
    public final int taskId;
    public final List<String> lines;

    public FetchLogResponse(int taskId, List<String> lines) {
        this.taskId = taskId;
        this.lines = lines;
    }
}

