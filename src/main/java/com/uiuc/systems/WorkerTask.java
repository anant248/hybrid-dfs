package com.uiuc.systems;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerTask {
    private final String leaderIp;

    private final int leaderPort;
    private final int taskId;
    private final String operator;
    private final List<String> operatorArgs;
    private final List<DownstreamTarget> downstream = new ArrayList<>();
    private final boolean isFinal;

    private Process operatorProc;
    private BufferedWriter opStdin;
    private BufferedReader opStdout;
    private int stageIdx;
    private String taskLogPath;
    private final Set<String> seenInputTuples = ConcurrentHashMap.newKeySet();
    private final AtomicLong nextTupleId = new AtomicLong(0);

    private static final ObjectMapper mapper = new ObjectMapper();

    HyDFS hdfs = GlobalHyDFS.hdfs;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkerTask.class);

    public WorkerTask(String leaderIp, int leaderPort, int taskId, String operator, boolean isFinal, List<String> operatorArgs, List<DownstreamTarget> downstream,int stageIdx) {
        this.leaderIp = leaderIp;
        this.leaderPort = leaderPort;
        this.taskId = taskId;
        this.operator = operator;
        this.isFinal = isFinal;
        this.operatorArgs = operatorArgs;
        this.downstream.addAll(downstream);
        this.stageIdx = stageIdx;
        this.taskLogPath = "/append_log/rainstorm_task_" + taskId + ".log";
        rebuildStateFromLog();
    }

    private long generateTupleId() {
        return nextTupleId.getAndIncrement();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WorkerTask <taskId> <operatorType> <isFinal> <operatorArgs...>");
            return;
        }

        int taskId = Integer.parseInt(args[0]);
        int stageIdx = Integer.parseInt(args[1]);
        String operatorType = args[2];
        int isFinalFlag = Integer.parseInt(args[3]);
        boolean isFinal = (isFinalFlag == 1);

        // All args after operator type are operator arguments
        List<String> operatorArgs = new ArrayList<>();
        for (int i = 4; i < args.length; i++) {
            operatorArgs.add(args[i]);
        }

        // Load downstream info—this would come from a config or leader message
        List<DownstreamTarget> downstream = RoutingLoader.load(taskId);

        WorkerTask worker = new WorkerTask(taskId, operatorType, isFinal, operatorArgs, downstream, stageIdx);
        worker.runTask();
    }

    public void runTask() throws Exception {
        launchOperator();

        System.out.println("WorkerTask " + taskId + " started operator " + operator);

        // Start stdout reader thread
        new Thread(this::stdoutReaderLoop).start();

        // Start input listener (TCP)
        startInputServer();
    }

    private void launchOperator() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add("python3");

        switch (operator) {
            case "filter": cmd.add("operator_filter.py"); break;
            case "transform": cmd.add("operator_transform.py"); break;
            case "aggregate": cmd.add("operator_aggregate.py"); break;
            case "identity": cmd.add("operator_identity.py"); break;
            default:
                System.err.println("Unknown operator: " + operator);
                System.exit(1);
        }

        cmd.addAll(operatorArgs);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        operatorProc = pb.start();
        opStdin = new BufferedWriter(new OutputStreamWriter(operatorProc.getOutputStream()));
        opStdout = new BufferedReader(new InputStreamReader(operatorProc.getInputStream()));
    }

    private void stdoutReaderLoop() {
        try {
            String line;
            while ((line = opStdout.readLine()) != null) {
                // if final task, write to HyDFS (stubbed here)
                if (isFinal) {
                    System.out.println(line);
                    //TODO: fix append call
                    hdfs.appendToHyDFS("stream_out.txt", line);
                } 
                // else, forward to downstream tasks
                else {
                    forwardTuple(line);
                }
            }
        } catch (Exception e) {
            System.err.println("Task " + taskId + ": operator stdout closed.");
        }
    }

    private void forwardTuple(String tuple) {
        for (DownstreamTarget t : downstream) {
            try (Socket socket = new Socket(t.host, t.port);
                 BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                w.write(tuple + "\n");
                w.flush();
            } catch (Exception e) {
                logger.error("Task {}: failed to send tuple to downstream task {} at {}:{}", taskId, t.getTaskId(), t.host, t.port, e);
                WorkerTaskFailRequest req = new WorkerTaskFailRequest(taskId,t.getTaskId());
                try(Socket socket = new Socket(leaderIp, leaderPort);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ){
                    out.writeObject(req);
                    out.flush();
                    logger.info("Task {} notified leader {}:{} of failure of {}", taskId, leaderIp, leaderPort, t.getTaskId());
                } catch (Exception err){
                    logger.error("Task {}: failed to notify leader", taskId, err);
                }
            }
        }
    }

    private void startInputServer() throws Exception {
        int listenPort = 9000 + taskId; // deterministic mapping
        try (ServerSocket server = new ServerSocket(listenPort)) {
            System.out.println("Task " + taskId + " listening on port " + listenPort);

            while (true) {
                Socket client = server.accept();
                new Thread(() -> handleClient(client)).start();
            }
        }
    }

//    private void handleClient(Socket client) {
//        try (BufferedReader r = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
//            String line;
//            while ((line = r.readLine()) != null) {
//                opStdin.write(line + "\n");
//                opStdin.flush();
//            }
//        } catch (Exception ignored) {}
//    }
    private void handleClient(Socket client) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (stageIdx == 0) {
                    long id = generateTupleId();
                    ObjectNode tuple = mapper.createObjectNode();
                    tuple.put("id", id);
                    tuple.put("line", line);
                    String json = tuple.toString();
                    seenInputTuples.add(Long.toString(id));
                    appendToTaskLog("INPUT " + id);
                    opStdin.write(json + "\n");
                    opStdin.flush();
                }
                else {
                    JsonNode js = mapper.readTree(line);
                    String tupleId = js.get("id").asText();
                    if (seenInputTuples.contains(tupleId)) {
                        logger.debug("Task {} dropping duplicate tuple {}", taskId, tupleId);
                        //TODO: SEND AN ACK EVEN IN THE CASE OF DUPLICATES
                        continue;
                    }
                    seenInputTuples.add(tupleId);
                    appendToTaskLog("INPUT " + tupleId);
                    opStdin.write(line + "\n");
                    opStdin.flush();
                }
            }
        } catch (Exception e) {
            logger.error("Task {} handleClient error", taskId, e);
        }
    }


    private void rebuildStateFromLog() {
        try {
            String hdfsName = "rainstorm_task_" + taskId + ".log";
            String localName = "task_" + taskId + "_log_local.txt";

            boolean ok = hdfs.getHyDFSFileToLocalFileFromOwner(hdfsName, localName);
            if (!ok) {
                logger.info("No prior log found for task {}", taskId);
                return;
            }
            List<String> lines = Files.readAllLines(Paths.get("output/" + localName));
            for (String line : lines) {
                if (line.startsWith("INPUT ")) {
                    String tupleId = line.split(" ")[1];
                    seenInputTuples.add(tupleId);
                }
            }
            logger.info("Task {} rebuilt state: {} tuples", taskId, seenInputTuples.size());

        } catch (Exception e) {
            logger.error("Task {} failed to rebuild state", taskId, e);
        }
    }


    private void appendToTaskLog(String logLine) {
        try {
            //TODO: fix append call
            hdfs.appendToHyDFS(taskLogPath, logLine + "\n");
        } catch (Exception e) {
            logger.error("Task {}: failed to append to HyDFS log {}", taskId, taskLogPath, e);
        }
    }
}

// Stub for fetching routing info
// TODO: get list of subsequent tasks from leader for this taskId
// eg. task 2 -> tasks 5,6,7
class RoutingLoader {
    public static List<DownstreamTarget> load(int taskId) {
        // leader will place routing files on VM
        List<DownstreamTarget> targets = new ArrayList<>();
        //Make sure this routing file path matches with what the server wrote to
        String filename = "/tmp/routing_" + taskId + ".conf";

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String first = br.readLine();
            int count = Integer.parseInt(first.split("=")[1]);
            for (int i = 0; i < count; i++) {
                String line = br.readLine();
                String[] s = line.split(":");
                String host = s[0];
                int port = Integer.parseInt(s[1]);
                int downstreamTaskId = Integer.parseInt(s[2]);
                targets.add(new DownstreamTarget(host, port, downstreamTaskId));
            }
        } catch (Exception e) {
            System.err.println("RoutingLoader: cannot load " + filename);
            e.printStackTrace();
        }
        return targets;
    }
}