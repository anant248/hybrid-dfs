package com.uiuc.systems;

import java.io.Serializable;

public class FetchLogRequest implements Serializable {
    public final int taskId;
    public FetchLogRequest(int taskId) {
        this.taskId = taskId;
    }

}
