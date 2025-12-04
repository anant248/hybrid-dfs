package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class StartWorkerTaskRequest implements Serializable {
    private int taskId;

    private int stageIdx;
    private String operator;
    private boolean isFinal;
    private List<String> operatorArgs;
    private String outputFileName;

    private String leaderIp;

    private int leaderPort;

    public StartWorkerTaskRequest(String leaderIp, int leaderPort, int taskId, int stageIdx, String operator, boolean isFinal, List<String> operatorArgs, String outputFileName) {
        this.leaderIp = leaderIp;
        this.leaderPort = leaderPort;
        this.taskId = taskId;
        this.stageIdx = stageIdx;
        this.operator = operator;
        this.isFinal = isFinal;
        this.operatorArgs = operatorArgs;
        this.outputFileName = outputFileName;
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

    public List<String> getOperatorArgs() {
        return operatorArgs;
    }

    public void setOperatorArgs(List<String> operatorArgs) {
        this.operatorArgs = operatorArgs;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }
}
