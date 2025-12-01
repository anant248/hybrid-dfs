package com.uiuc.systems;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RainStormLeader {
    NodeId current;
    public static final int LEADER_PORT = 7777;
    public static final int WORKER_PORT = 7979;
    private final int numStages;
    private final int numTasks;
    private final boolean exactlyOnce;
    private final boolean autoscaleEnabled;
    private final int inputRate;
    private final int lw;
    private final int hw;

    private final String leaderHost;

    private final List<String> vmHosts = Arrays.asList("vm1", "vm2", "vm3", "vm4", "vm5", "vm6", "vm7", "vm8", "vm9", "vm10");

    static class TaskInfo {
        final int globalTaskId;
        final int stageIdx;
        final int idxWithinStage;
        final String host;

        TaskInfo(int globalTaskId, int stageIdx, int idxWithinStage, String hostIP) {
            this.globalTaskId = globalTaskId;
            this.stageIdx = stageIdx;
            this.idxWithinStage = idxWithinStage;
            this.host = hostIP;
        }
    }

    private final Map<Integer, TaskInfo> tasks = new HashMap<>();

    private RainStormLeader(String leaderHost, int numStages, int numTasks, boolean exactlyOnce, boolean autoscaleEnabled, int inputRate, int lw, int hw) {
        this.numStages = numStages;
        this.numTasks = numTasks;
        this.exactlyOnce = exactlyOnce;
        this.autoscaleEnabled = autoscaleEnabled;
        this.inputRate = inputRate;
        this.lw = lw;
        this.hw = hw;
        this.leaderHost = leaderHost;
    }

//    private static RainStormLeader parseArgs(String[] args) {
//        int nStages = Integer.parseInt(args[0]);
//        int nTasksPerStage = Integer.parseInt(args[1]);
//        int split = args.length - 7;
//        boolean exactlyOnce = Boolean.parseBoolean(args[split + 2]);
//        boolean autoscaleEnabled = Boolean.parseBoolean(args[split + 3]);
//        int inputRate = Integer.parseInt(args[split + 4]);
//        int lw = Integer.parseInt(args[split + 5]);
//        int hw = Integer.parseInt(args[split + 6]);
//
//        LeaderLoggerHelper.runStart(String.join(" ", args));
//        LeaderLoggerHelper.config(nStages, nTasksPerStage, exactlyOnce, autoscaleEnabled, inputRate, lw, hw);
//        return new RainStormLeader(nStages, nTasksPerStage, exactlyOnce, autoscaleEnabled, inputRate, lw, hw);
//    }

    public void run() throws Exception {
        new Thread(new LeaderServer(this, LEADER_PORT)).start();
        initializeTasks();
        distributeRoutingFiles();
        launchAllWorkerTasks();
        Thread.sleep(2000);
        LeaderLoggerHelper.runEnd("OK");
    }
    private void initializeTasks() {
//        List<String> ips = Arrays.asList("vm1", "vm2", "vm3", "vm4", "vm5", "vm6","vm7","vm8","vm9","vm10");
        int id = 0;
        for (int stage = 0; stage < numStages; stage++) {
            for (int i = 0; i < numTasks; i++) {
                String vm = vmHosts.get(id % vmHosts.size());
                TaskInfo ti = new TaskInfo(id,stage,i,vm);
                tasks.put(id, ti);
                LeaderLoggerHelper.taskStart(stage,id,vm);
                id++;
            }
        }
    }

    private void distributeRoutingFiles() {
        for (TaskInfo t : tasks.values()) {
            sendRoutingFileToTask(t);
        }
    }

    private String buildRoutingFile(TaskInfo t) {
        StringBuilder sb = new StringBuilder();
        if (t.stageIdx == numStages - 1) {
            sb.append("downstreamCount=0\n");
            return sb.toString();
        }
        sb.append("downstreamCount=" + numTasks + "\n");
        int nextStageStart=(t.stageIdx+1)*numTasks;
        for (int i = 0; i < numTasks; i++) {
            int downstreamId = nextStageStart + i;
            TaskInfo downstream = tasks.get(downstreamId);
            int port = 9000 + downstream.globalTaskId;
            sb.append(downstream.host + ":" + port + ":" + downstream.globalTaskId + "\n");
        }
        return sb.toString();
    }

    private void launchAllWorkerTasks() throws Exception {
        for (TaskInfo t : tasks.values()) {
            launchSingleWorkerTask(t);
        }
    }

    private void launchSingleWorkerTask(TaskInfo t) {
        boolean isFinal = (t.stageIdx == numStages - 1);
        StartWorkerTaskRequest req = new StartWorkerTaskRequest(leaderHost, LEADER_PORT, t.globalTaskId, t.stageIdx, chooseOperator(t.stageIdx), isFinal, operatorArgs(t.stageIdx));

        try (Socket socket = new Socket(t.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(req);
            out.flush();
        } catch (Exception e) {
            System.out.println("There was an error while sending the start request to the worker task " + t.globalTaskId);
        }
    }

    private String chooseNewHost(String oldHost) {
        int idx = vmHosts.indexOf(oldHost);
        if (idx == -1) {
            return vmHosts.get(0);
        }
        int newIdx = (idx + 1) % vmHosts.size();
        return vmHosts.get(newIdx);
    }

    private void restartFailedTask(int failedTaskId) {
        TaskInfo old = tasks.get(failedTaskId);
        if (old == null) {
            System.err.println("Leader: restartFailedTask called for unknown taskId " + failedTaskId);
            return;
        }

        String newHost = chooseNewHost(old.host);
        TaskInfo updated = new TaskInfo(old.globalTaskId, old.stageIdx, old.idxWithinStage, newHost);
        tasks.put(failedTaskId, updated);
        sendRoutingFileToTask(updated);

        if (updated.stageIdx > 0) {
            int upstreamStage = updated.stageIdx - 1;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == upstreamStage) {
                    sendRoutingFileToTask(t);
                }
            }
        }
        launchSingleWorkerTask(updated);
        LeaderLoggerHelper.taskRestart(updated.stageIdx, updated.globalTaskId, newHost);
    }

    private String chooseOperator(int stage) {
        //TODO: Modify this method later
        return "identity";
    }

    private List<String> operatorArgs(int stage) {
        //TODO: Modify this method later
        return List.of();
    }

    private void sendRoutingFileToTask(TaskInfo t) {
        String routing = buildRoutingFile(t);
        WorkerTaskRoutingFileRequest req = new WorkerTaskRoutingFileRequest(routing, t.globalTaskId);

        try (Socket socket = new Socket(t.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(req);
            out.flush();
        } catch (Exception e) {
            System.out.println("Encountered an error while sending the routing file to the worker node " + t.globalTaskId);
        }
    }

    public void workerTaskFailureHandler(WorkerTaskFailRequest req){
        TaskInfo failedTaskInfo = tasks.get(req.getFailedTaskId());
        if (failedTaskInfo == null) {
            System.err.println("Leader: received failure for unknown task " + req.getFailedTaskId());
            return;
        }
        LeaderLoggerHelper.taskFail(failedTaskInfo.idxWithinStage,failedTaskInfo.globalTaskId,failedTaskInfo.host);
        //TODO: what you wanna do after logging?
        restartFailedTask(req.getFailedTaskId());
    }
}
