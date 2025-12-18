package com.uiuc.systems;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import java.io.*;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/*
  This is a Rainstorm leader class. Rainstorm leader is responsible for creating the worker task processes for each stage and assigning them to
  run on a particular host and it uses modulo operation to decide which host a worker task should run on.
  It also sends the downstream routing configuration to the hosts that these worker tasks to run on, so that
  the task can know who to send it's output to after it has processed the tuple. The leader is responsible for
  streaming the tuples from the input file and it will be sent to the tasks in the next stage.

  The leader handles auto-scaling operations like scale up or scale down based on the average number
  of tuples a task processed. The leader also handles when a worker task has failed, it tries to restart
  that failed worker task in a new host and also sends the downstream routing configuration to the tasks in
  the previous stage.
*  */
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

    private final Logger leaderLogger;

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
    private final Map<Integer, Long> lastScaleTime = new ConcurrentHashMap<>();

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
        this.leaderLogger = LeaderLoggerFactory.createLeaderLogger();
        leaderLogger.info("RainStorm Leader started");
        LeaderLoggerHelper.init(this.leaderLogger);
    }

    /* The main class will call this run() method to kickoff RainStorm. This will start up the leader server, distribute downstream
       routing info for all the tasks, stream the input tuples, launch all the worker tasks and also keep track of the number of
       tuples processed/sec by the worker tasks to calculate the average and use that when autoscaling is enabled.
    */
    public void run() throws Exception {
        LeaderLoggerHelper.runStart();
        System.out.println("RainStorm Leader starting on host " + leaderHost);

        // create an empty output file in HyDFS of this VM (Rainstorm leader VM -- always VM1)
        try (FileOutputStream fos = new FileOutputStream("hdfs/" + hydfsDestFileName)) {
            fos.write(0);
            System.out.println("Leader: created destination file " + hydfsDestFileName + " in HyDFS");

        } catch (Exception e) {
            leaderLogger.error("Leader: failed to create destination file {} in HyDFS", hydfsDestFileName, e);
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
                System.err.println("No tasks in Stage 0, cannot source");
                return;
            }

            // Open sockets to all stage-0 tasks
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

            long intervalNs = 1_000_000_000L / inputRate;
            long nextSendTime = System.nanoTime();

            try (BufferedReader br = new BufferedReader(
                    new FileReader(Paths.get("inputs/" + localInput).toString()))) {

                String line;
                while ((line = br.readLine()) != null) {
                    long now = System.nanoTime();
                    if (now < nextSendTime) {
                        long sleepNs = nextSendTime - now;
                        Thread.sleep(
                                sleepNs / 1_000_000,
                                (int) (sleepNs % 1_000_000));
                    }
                    nextSendTime += intervalNs;
                    int target = lineNum % stage0Tasks;
                    BufferedWriter w = writers.get(target);

                    long tid = nextGlobalTupleId.getAndIncrement();
                    ObjectNode js = mapper.createObjectNode();
                    js.put("id", tid);
                    js.put("key", localInput + ":" + lineNum);
                    js.put("line", line);
                    js.put("srcTask", -1);

                    w.write(js.toString());
                    w.write("\n");
                    w.flush();

                    lineNum++;
                }
            } catch (Exception e) {
                leaderLogger.error("SourceProcess: encountered error while streaming input", e);
            }
            for (BufferedWriter w : writers) w.close();
            for (Socket s : sockets) s.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void distributeRoutingFiles() {
        for (TaskInfo t : tasks.values()) {
            sendRoutingFileToTask(t);
        }
    }


    // This method is used to build downstream routing info which the tasks will use to send the tuples to
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
            int port = 9000 + d.globalTaskId;
            sb.append(d.host).append(":").append(port).append(":").append(d.globalTaskId).append("\n");
        }
        return sb.toString();
    }


    // Launch all worker tasks in all stages
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
            leaderLogger.error("There was an error while sending the start request to the worker task " + t.globalTaskId);
        }
    }

    // This method is used to determine which host the worker task should run on
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
            leaderLogger.error("Failed to send kill request: " + e.getMessage());
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

    // This method is invoked when a worker task has failed or was killed and it needs to be restarted and executed on a different host
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

    // This method is used to open a socket on the worker task server to send the downstream routing info
    private void sendRoutingFileToTask(TaskInfo t) {
        String routing = buildRoutingFile(t);
        WorkerTaskRoutingFileRequest req = new WorkerTaskRoutingFileRequest(routing, t.globalTaskId);

        try (Socket socket = new Socket(t.host, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(req);
            out.flush();
        } catch (Exception e) {
            leaderLogger.error("Encountered an error while sending the routing file to the worker node " + t.globalTaskId);
        }
    }

    // This method will handle a worker task failure and call restart
    public void workerTaskFailureHandler(WorkerTaskFailRequest req){
        TaskInfo failedTaskInfo = tasks.get(req.getFailedTaskId());
        if (failedTaskInfo == null) {
            System.err.println("Leader: received failure for unknown task " + req.getFailedTaskId());
            return;
        }
        LeaderLoggerHelper.taskFail(failedTaskInfo.idxWithinStage,failedTaskInfo.globalTaskId,failedTaskInfo.host);
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

    // This method is used to calculate the average number of tuples each task has processed which is used in autoscaling
    private void processLoad() {
        long now = System.currentTimeMillis();
        for (int stage = 0; stage < numStages; stage++) {
            if ("aggregate".equalsIgnoreCase(getStageOperator(stage))) continue;
            long last = lastScaleTime.getOrDefault(stage, 0L);
            if (now - last < 3000) {
                continue;
            }
            long total = 0;
            int count = 0;
            for (TaskInfo t : tasks.values()) {
                if (t.stageIdx == stage) {
                    if (!workerTaskLoad.containsKey(t.globalTaskId)) {
                        continue;
                    }

                    total += workerTaskLoad.get(t.globalTaskId);
                    count++;
                }
            }
            if (count == 0) continue;
            long avg = total / count;
            if (avg < lw && count > 1) {
                scaleDown(stage);
                lastScaleTime.put(stage, System.currentTimeMillis());
            }
            else if (avg > hw) {
                scaleUp(stage);
                lastScaleTime.put(stage, System.currentTimeMillis());
            }
        }
    }

    // This method is invoked when the avg tuples/sec is greater than the higher watermark
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

    // This method is invoked when the avg tuples/sec is lower than the lower watermark
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
            leaderLogger.error("Failed to send KillWorkerTask request for task " + workerTaskIdToBeKilled);
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

    // This method is invoked to fetch the failed worker task log

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

    // This method is invoked to install the old failed worker task log on a new host that task will now run on
    private void installLog(String newHost, int taskId, List<String> lines) {
        try (Socket s = new Socket(newHost, WORKER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream());) {

            out.writeObject(new InstallLog(taskId, lines));
            out.flush();

            String response = (String) in.readObject();

        } catch (Exception ignored) {}
    }

    /* Getter method to return an immutable copy of the tasks map */
    public Map<Integer, TaskInfo> getTasks() {
        return new HashMap<>(this.tasks);
    }
}
