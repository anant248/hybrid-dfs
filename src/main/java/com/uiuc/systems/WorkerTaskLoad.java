package com.uiuc.systems;

import java.io.Serializable;

public class WorkerTaskLoad implements Serializable {
    private int taskIdx;
    private int stageIdx;
    private long tuplesCount;

    public WorkerTaskLoad(int taskIdx, int stageIdx, long tuplesCount) {
        this.taskIdx = taskIdx;
        this.stageIdx = stageIdx;
        this.tuplesCount = tuplesCount;
    }

    public int getTaskIdx() {
        return taskIdx;
    }

    public void setTaskIdx(int taskIdx) {
        this.taskIdx = taskIdx;
    }

    public int getStageIdx() {
        return stageIdx;
    }

    public void setStageIdx(int stageIdx) {
        this.stageIdx = stageIdx;
    }

    public long getTuplesCount() {
        return tuplesCount;
    }

    public void setTuplesCount(long tuplesCount) {
        this.tuplesCount = tuplesCount;
    }
}
