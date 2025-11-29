package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;

public class StartWorkerTaskRequest implements Serializable {
    private int taskId;
    private String operator;
    private boolean isFinal;
    private List<String> operatorArgs;

    public StartWorkerTaskRequest(int taskId, String operator, boolean isFinal, List<String> operatorArgs) {
        this.taskId = taskId;
        this.operator = operator;
        this.isFinal = isFinal;
        this.operatorArgs = operatorArgs;
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
}
