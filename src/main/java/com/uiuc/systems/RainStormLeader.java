package com.uiuc.systems;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RainStormLeader {
    public static final int LEADER_PORT = 7777;
    public static final int WORKER_PORT = 7979;
    private final int numStages;
    private final int numTasks;
    private final boolean exactlyOnce;
    private final boolean autoscaleEnabled;
    private final int inputRate;
    private final int lw;
    private final int hw;
    private final String localInputFileName;
    private final String hydfsDestFileName;
    private final List<String> operatorsAndArgs;
    private Ring ring;

    private final String leaderHost;

    private final List<String> vmHosts = Arrays.asList("fa25-cs425-7602.cs.illinois.edu", "fa25-cs425-7603.cs.illinois.edu", "fa25-cs425-7604.cs.illinois.edu", "fa25-cs425-7605.cs.illinois.edu", "fa25-cs425-7606.cs.illinois.edu", "fa25-cs425-7607.cs.illinois.edu", "fa25-cs425-7608.cs.illinois.edu", "fa25-cs425-7609.cs.illinois.edu", "fa25-cs425-7610.cs.illinois.edu");

    private final ConcurrentMap<Integer, Long> workerTaskLoad = new ConcurrentHashMap<>();

    private final AtomicLong nextGlobalTupleId = new AtomicLong(0);

    HyDFS hdfs = GlobalHyDFS.getHdfs();

    private static final Logger logger = LoggerFactory.getLogger(RainStormLeader.class);

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

    public RainStormLeader(String leaderHost, int numStages, int numTasks, boolean exactlyOnce, boolean autoscaleEnabled, int inputRate, int lw, int hw,  List<String> operatorsAndArgs, String localInputFileName, String hydfsDestFileName, Ring ring) {
        this.numStages = numStages;
        this.numTasks = numTasks;
        this.exactlyOnce = exactlyOnce;
        this.autoscaleEnabled = autoscaleEnabled;
        this.inputRate = inputRate;
        this.lw = lw;
        this.hw = hw;
        this.leaderHost = leaderHost;
        this.operatorsAndArgs = operatorsAndArgs;
        this.localInputFileName = localInputFileName;
        this.hydfsDestFileName = hydfsDestFileName;
        this.ring = ring;
    }

    public void run() throws Exception {
        LeaderLoggerHelper.runStart();
        System.out.println("RainStorm Leader starting on host " + leaderHost);

        // create an empty output file in HyDFS of this VM (Rainstorm leader VM -- always VM1)
        try (FileOutputStream fos = new FileOutputStream("hdfs/" + hydfsDestFileName)) {
            fos.write(0);
            System.out.println("Leader: created destination file " + hydfsDestFileName + " in HyDFS");

        } catch (Exception e) {
            logger.error("Leader: failed to create destination file {} in HyDFS", hydfsDestFileName, e);
        }

        new Thread(new LeaderServer(this, LEADER_PORT)).start();
        initializeTasks();
        distributeRoutingFiles();
        launchAllWorkerTasks();
        if(autoscaleEnabled && !exactlyOnce){
            Thread rm = new Thread(this::adjustLoad);
            rm.setDaemon(true);
            rm.start();
        }
        Thread.sleep(5000);
        new Thread(() -> runSourceProcess(localInputFileName)).start();

        // LeaderLoggerHelper.runEnd();
        // System.out.println("RainStorm Leader finished execution.");
    }
    private void initializeTasks() {
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

    private void runSourceProcess(String localInput) {
        try {
            int stage0Tasks = 0;
            List<TaskInfo> stage0List = new ArrayList<>();

            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == 0) {
                    stage0List.add(t);
                    stage0Tasks++;
                }
            }

            if (stage0Tasks == 0) {
                System.err.println("No tasks in Stage 0 — cannot source.");
                return;
            }


            // for each task in stage0List, open a socket and keep it open
            List<Socket> sockets = new ArrayList<>();
            List<BufferedWriter> writers = new ArrayList<>();

            for (TaskInfo t : stage0List) {
                try {
                    Socket s = new Socket(t.host, 9000 + t.globalTaskId);
                    sockets.add(s);
                    writers.add(new BufferedWriter(new OutputStreamWriter(s.getOutputStream())));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("SourceProcess: started streaming input to " + stage0Tasks + " Stage 0 tasks.");
            int lineNum = 0;
            ObjectMapper mapper = new ObjectMapper();
            long sleepMicros = (long)(1_000_000.0 / inputRate); // enforce input rate

            try (BufferedReader br = new BufferedReader(new FileReader(Paths.get("inputs/" + localInput).toString()))) {
                String line;
                while ((line = br.readLine()) != null) {

                    int target = lineNum % stage0Tasks;
                    BufferedWriter w = writers.get(target);

                    // Build tuple
                    long tid = nextGlobalTupleId.getAndIncrement();
                    ObjectNode js = mapper.createObjectNode();
                    js.put("id", tid);
                    js.put("key", localInput + ":" + lineNum);
                    js.put("line", line);
                    js.put("srcTask", -1);

                    // Print if we are sending the tuple
                    System.out.println("SourceProcess: sending tuple of id" + tid + " to Task " + stage0List.get(target).globalTaskId);
                    // System.out.println(js.toString() + "\n");

                    w.write(js.toString() + "\n");
                    w.flush();

                    lineNum++;
                    Thread.sleep(0, (int)sleepMicros);
                }

            } catch (Exception e) {
                logger.error("SourceProcess: encountered error while streaming input", e);
                e.printStackTrace();
            }

            System.out.println("SourceProcess: finished streaming all input lines.");
            
            // close the sockets and writers
            for (BufferedWriter w : writers) w.close();
            for (Socket s : sockets) s.close();
        }
        catch (Exception e) {
            e.printStackTrace();
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
        int nextStage = t.stageIdx + 1;
        List<TaskInfo> downstreamTasks = new java.util.ArrayList<>();
        for (TaskInfo info : tasks.values()) {
            if (info.stageIdx == nextStage) {
                downstreamTasks.add(info);
            }
        }
        sb.append("downstreamCount=").append(downstreamTasks.size()).append("\n");
        for (TaskInfo d : downstreamTasks) {
            //TODO: Double-check the worker task port
            int port = 9000 + d.globalTaskId;
            sb.append(d.host).append(":").append(port).append(":").append(d.globalTaskId).append("\n");
        }
        return sb.toString();
    }

    private void launchAllWorkerTasks() throws Exception {
        for (int i = 0; i < numStages; i++) {
            // get operator type and args for this task we are starting based on its stage and the initial operatorsAndArgs list
            String operatorType = getStageOperator(i);
            String operatorArgs = getStageOperatorArgs(i);

            for (int j = 0; j < numTasks; j++) {
                TaskInfo t = tasks.get(i * numTasks + j); // get the task based on stage and index within stage
                launchSingleWorkerTask(t, operatorType, operatorArgs);
            }
        }
    }

    private void launchSingleWorkerTask(TaskInfo t, String operatorType, String operatorArgs) {
        boolean isFinal = (t.stageIdx == numStages - 1);
        StartWorkerTaskRequest req = new StartWorkerTaskRequest(leaderHost, LEADER_PORT, t.globalTaskId, t.stageIdx, operatorType, isFinal, operatorArgs, hydfsDestFileName, ring, t.host);

        System.out.println("Launching WorkerTask " + t.globalTaskId + " on host " + t.host + " for stage " + t.stageIdx + " with operator " + operatorType);

        try (Socket socket = new Socket(t.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(req);
            out.flush();
        } catch (Exception e) {
            System.out.println("There was an error while sending the start request to the worker task " + t.globalTaskId);
        }
    }

    private String getNewIp(String oldHost) {
        int idx = vmHosts.indexOf(oldHost);
        if (idx == -1) {
            return vmHosts.get(0);
        }
        int newIdx = (idx + 1) % vmHosts.size();
        return vmHosts.get(newIdx);
    }

    // given a stage number return the operator type for that stage
    public String getStageOperator(int stageIdx) {
        int index = stageIdx * 2; // each stage has 2 entries: operator type and args
        if (index >= operatorsAndArgs.size()) {
            return null;
        }
        return operatorsAndArgs.get(index);
    }

    // given a stage number return the operator args for that stage
    public String getStageOperatorArgs(int stageIdx) {
        int index = stageIdx * 2 + 1; // the index after the operator type has the args for that operator
        if (index >= operatorsAndArgs.size()) {
            return null;
        }
        return operatorsAndArgs.get(index);
    }
    
    // method invoked by Leader to send kill request to WorkerTaskServer on the worker VM
    public void killTask(String vmIp, int taskId, int pid) {
        boolean killSuccess = false;

        // send kill request to WorkerTaskServer on vmIp
        try (Socket socket = new Socket(vmIp, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        ) {
            out.writeObject(new KillWorkerTask(taskId));
            out.flush();
            System.out.println("Kill request sent for Task " + taskId + " at PID " + pid + " on " + vmIp);
            WorkerTaskKillAckRequest ack = (WorkerTaskKillAckRequest) in.readObject();
            killSuccess = ack.isKilled();
            System.out.println("Kill success: "+ taskId);
        } catch (Exception e) {
            System.out.println("Failed to send kill request: " + e.getMessage());
        }

        // check if kill was successful
        if (!killSuccess) {
            System.err.println("Leader: Kill NOT confirmed for task " + taskId + "");
            return;
        }

        // create WorkerTaskFailRequest that workerTaskFailureHandler can use
        // note: currentTaskId is not used in handler so we dont care who detected the failure
        WorkerTaskFailRequest req = new WorkerTaskFailRequest(0, taskId);

        // begin process of restarting a new task to replace the killed task
        workerTaskFailureHandler(req);
    }

    private void restartFailedTask(int failedTaskId) {
        TaskInfo old = tasks.get(failedTaskId);
        if (old == null) {
            System.err.println("Leader: restartFailedTask called for unknown taskId " + failedTaskId);
            return;
        }
        String oldHost = old.host;

        List<String> lines = fetchLog(oldHost, failedTaskId);

        String newHost = getNewIp(old.host);

        installLog(newHost, failedTaskId, lines);

        TaskInfo updated = new TaskInfo(old.globalTaskId, old.stageIdx, old.idxWithinStage, newHost);
        tasks.put(failedTaskId, updated);

        System.out.println("Restarting failed task " + failedTaskId + " on new host " + newHost);
        sendRoutingFileToTask(updated);

        if (updated.stageIdx > 0) {
            int upstreamStage = updated.stageIdx - 1;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == upstreamStage) {
                    sendRoutingFileToTask(t);
                }
            }
        }

        // get operator type and args for this task we are starting based on its stage and the initial operatorsAndArgs list
        String operatorType = getStageOperator(updated.stageIdx);
        String operatorArgs = getStageOperatorArgs(updated.stageIdx);

        launchSingleWorkerTask(updated, operatorType, operatorArgs); // launch the single worker task with the correct operator type and args on the new host
        LeaderLoggerHelper.taskRestart(updated.stageIdx, updated.globalTaskId, newHost);
    }

    private void sendRoutingFileToTask(TaskInfo t) {
        String routing = buildRoutingFile(t);
        WorkerTaskRoutingFileRequest req = new WorkerTaskRoutingFileRequest(routing, t.globalTaskId);

        try (Socket socket = new Socket(t.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(req);
            System.out.println("Sent routing file to task " + t.globalTaskId + " on host " + t.host);
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

    public void processWorkerTaskLoad(WorkerTaskLoad req){
        workerTaskLoad.put(req.getTaskIdx(), req.getTuplesCount());
    }

    private void adjustLoad() {
        while (autoscaleEnabled) {
            try {
                Thread.sleep(1000);
                processLoad();
            } catch (Exception ignored) {}
        }
    }

    private void processLoad() {
        for (int stage = 0; stage < numStages; stage++) {

            // Skip auto-scaling for aggregate stages (stateful stage)
            if ("aggregate".equalsIgnoreCase(getStageOperator(stage))) continue;

            long total = 0;
            int count = 0;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == stage) {
                    total += workerTaskLoad.getOrDefault(t.globalTaskId, 0L);
                    count++;
                }
            }
            if (count == 0) continue;
            long avg = total / count;
            if (avg < lw && count > 1) {
                scaleDown(stage);
            } else if (avg > hw) {
                scaleUp(stage);
            }
        }
    }

    private void scaleUp(int stage) {
        int newTaskId = tasks.keySet().stream().mapToInt(x -> x).max().orElse(-1) + 1;
        String newHost = vmHosts.get(newTaskId % vmHosts.size());
        int idxWithinStage = 0;
        for (TaskInfo t : tasks.values()) {
            if (t.stageIdx == stage) {
                idxWithinStage++;
            }
        }

        TaskInfo ti = new TaskInfo(newTaskId,stage,idxWithinStage,newHost);
        tasks.put(newTaskId, ti);
        LeaderLoggerHelper.taskScaleUp(stage,newTaskId,newHost);
        sendRoutingFileToTask(ti);
        if (stage > 0) {
            int upstreamStage = stage - 1;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == upstreamStage) {
                    sendRoutingFileToTask(t);
                }
            }
        }

        // get operator type and args for this task we are starting based on its stage and the initial operatorsAndArgs list
        String operatorType = getStageOperator(stage);
        String operatorArgs = getStageOperatorArgs(stage);

        launchSingleWorkerTask(ti, operatorType, operatorArgs);
    }

    private void scaleDown(int stage) {
        TaskInfo taskToBeKilled = null;
        for (TaskInfo t : tasks.values()) {
            if (t.stageIdx == stage) {
                if (taskToBeKilled == null || t.globalTaskId > taskToBeKilled.globalTaskId) {
                    taskToBeKilled = t;
                }
            }
        }
        if (taskToBeKilled == null) return;
        boolean killSuccess = false;
        int workerTaskIdToBeKilled = taskToBeKilled.globalTaskId;

        try (Socket socket = new Socket(taskToBeKilled.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        ) {
            out.writeObject(new KillWorkerTask(workerTaskIdToBeKilled));
            out.flush();
            WorkerTaskKillAckRequest ack = (WorkerTaskKillAckRequest) in.readObject();
            killSuccess = ack.isKilled();
        } catch (Exception e) {
            System.err.println("Failed to send KillWorkerTask request for task " + workerTaskIdToBeKilled);
            return;
        }
        if (!killSuccess) {
            System.err.println("Leader: Kill NOT confirmed for task " + workerTaskIdToBeKilled + ". Aborting scale-down.");
            return;
        }
        tasks.remove(workerTaskIdToBeKilled);
        workerTaskLoad.remove(workerTaskIdToBeKilled);
        LeaderLoggerHelper.taskScaleDown(stage, workerTaskIdToBeKilled, taskToBeKilled.host);
        if (stage > 0) {
            int upstreamStage = stage-1;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == upstreamStage) {
                    sendRoutingFileToTask(t);
                }
            }
        }
    }

    private List<String> fetchLog(String oldHost, int taskId) {
        try (Socket s = new Socket(oldHost, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new FetchLogRequest(taskId));
            out.flush();

            FetchLogResponse resp = (FetchLogResponse) in.readObject();
            return resp.lines;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private void installLog(String newHost, int taskId, List<String> lines) {
        try (Socket s = new Socket(newHost, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream());) {

            out.writeObject(new InstallLog(taskId, lines));
            out.flush();

            //TODO: IS THIS NECESSARY?
            String response = (String) in.readObject();
            if(response.equals("OK")){
                System.out.println("Successfully installed the old log in the new vm for the task ID: "+taskId);
            }
            else{
                System.out.println("An error occurred while installing the olg log file in the new vm for the task ID: "+taskId);
            }

        } catch (Exception ignored) {}
    }

    /* Getter method to return an immutable copy of the tasks map */
    public Map<Integer, TaskInfo> getTasks() {
        return new HashMap<>(this.tasks);
    }

    /*
     * Method to list all current tasks with their VM, PID, global task id, operator executed, and local log file name
    */
}
