package com.uiuc.systems;

import java.io.Serializable;

public class StartWorkerTaskRequest implements Serializable {
    private int taskId;

    private int stageIdx;
    private String operator;
    private boolean isFinal;
    private String operatorArgs;
    private String outputFileName;
    private String workerHost;
    private Ring ring;

    private String leaderIp;

    private int leaderPort;

    public StartWorkerTaskRequest(String leaderIp, int leaderPort, int taskId, int stageIdx, String operator, boolean isFinal, String operatorArgs, String outputFileName, Ring ring, String workerHost) {
        this.leaderIp = leaderIp;
        this.leaderPort = leaderPort;
        this.taskId = taskId;
        this.stageIdx = stageIdx;
        this.operator = operator;
        this.isFinal = isFinal;
        this.operatorArgs = operatorArgs;
        this.outputFileName = outputFileName;
        this.workerHost = workerHost;
        this.ring = ring;
    }

    public int getStageIdx() {
        return stageIdx;
    }

    public String getLeaderIp() {
        return leaderIp;
    }

    public void setLeaderIp(String leaderIp) {
        this.leaderIp = leaderIp;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public void setLeaderPort(int leaderPort) {
        this.leaderPort = leaderPort;
    }

    public void setStageIdx(int stageIdx) {
        this.stageIdx = stageIdx;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getWorkerHost() {
        return workerHost;
    }

    public Ring getRing() {
        return ring;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }

    public void setWorkerHost(String workerHost) {
        this.workerHost = workerHost;
    }

    public String getOperatorArgs() {
        return operatorArgs;
    }

    public void setOperatorArgs(String operatorArgs) {
        this.operatorArgs = operatorArgs;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }
}
