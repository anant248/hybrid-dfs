package com.uiuc.systems;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerTask {
    private final String leaderIp;
    private static final int WORKER_TASK_PORT = 6971;
    private final int leaderPort;
    private final int taskId;
    private final String operator;
    private final String operatorArgs;
    private final List<DownstreamTarget> downstream = new ArrayList<>();
    private final boolean isFinal;
    private final String OUTPUT_FILE;

    private Process operatorProc;
    private BufferedWriter opStdin;
    private BufferedReader opStdout;
    private int stageIdx;
    private String taskLogPath;
    private final Set<String> seenInputTuples = ConcurrentHashMap.newKeySet();

    private static final ObjectMapper mapper = new ObjectMapper();

    private HyDFS hdfs;
    private static final List<String> ips = Arrays.asList("fa25-cs425-7602.cs.illinois.edu", "fa25-cs425-7603.cs.illinois.edu", "fa25-cs425-7604.cs.illinois.edu", "fa25-cs425-7605.cs.illinois.edu", "fa25-cs425-7606.cs.illinois.edu", "fa25-cs425-7607.cs.illinois.edu", "fa25-cs425-7608.cs.illinois.edu", "fa25-cs425-7609.cs.illinois.edu", "fa25-cs425-7610.cs.illinois.edu");

    private final AtomicLong tuplesCount = new AtomicLong(0);

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkerTask.class);

    static class PendingTuple implements Serializable {
        final String tupleId;
        final String json;
        volatile long lastSentTime;
        volatile int retryCount;

        PendingTuple(String tupleId, String json, int retryCount) {
            this.tupleId = tupleId;
            this.json = json;
            this.lastSentTime = System.currentTimeMillis();
            this.retryCount = retryCount;
        }
    }

    private final ConcurrentHashMap<String, PendingTuple> pendingTuples = new ConcurrentHashMap<>();
    private static final long RETRY_INTERVAL = 2000;

    private static final int MAX_RETRIES = 3;

    public WorkerTask(String leaderIp, int leaderPort, int taskId, String operator, boolean isFinal, String operatorArgs, List<DownstreamTarget> downstream,int stageIdx, String outputFile, Ring ring, String currentIp) {
        this.leaderIp = leaderIp;
        this.leaderPort = leaderPort;
        this.taskId = taskId;
        this.operator = operator;
        this.isFinal = isFinal;
        this.operatorArgs = operatorArgs;
        this.downstream.addAll(downstream);
        this.stageIdx = stageIdx;
        this.OUTPUT_FILE = outputFile;
        this.taskLogPath = "rainstorm_task_" + taskId + ".log";

        // build out HyDFS for this worker task
        NodeId currentNode = new NodeId(currentIp, WORKER_TASK_PORT, System.currentTimeMillis());
        this.hdfs = new HyDFS(currentNode, ring);

        // if rebuildState was false, create an empty log file in HyDFS, otherwise the appends will fail
        // if rebuildState was true, the log file already exists
        if (!rebuildStateFromLog()) {
            // create an empty log file in HyDFS of this VM
            try (FileOutputStream fos = new FileOutputStream("hdfs/" + taskLogPath)){               
                fos.write(0);
                System.out.println("Task " + taskId + ": created new HyDFS log file " + taskLogPath);

            } catch (Exception e) {
                logger.error("Task {}: failed to create HyDFS log file {}", taskId, taskLogPath, e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WorkerTask <leader IP> <leader port> <taskId> <stageIdx> <operatorType> <isFinal> <outputFileName> <serialized ring object> <workerHost IP> [operatorArgs...]");
            return;
        }

        System.out.println("Starting WorkerTask with args: " + String.join(" ", args) + "and worker task args count: " + (args.length));

        String leaderIp = args[0];
        int leaderPort = Integer.parseInt(args[1]);
        int taskId = Integer.parseInt(args[2]);
        int stageIdx = Integer.parseInt(args[3]);
        String operatorType = args[4];
        int isFinalFlag = Integer.parseInt(args[5]);
        boolean isFinal = (isFinalFlag == 1);
        String outputFile = args[6];
        String ringJson = args[7];
        String workerHost = args[8];
        String operatorArgs = args[9];

        System.out.println("Worker Task Main Args Parsed: leaderIp=" + leaderIp + ", leaderPort=" + leaderPort + ", taskId=" + taskId + ", stageIdx=" + stageIdx + ", operatorType=" + operatorType + ", isFinal=" + isFinal + ", outputFile=" + outputFile + ", workerHost=" + workerHost + ", operatorArgs=" + operatorArgs + ", ringJson=" + ringJson);

        // Load downstream info—this would come from a config or leader message
        List<DownstreamTarget> downstream = RoutingLoader.load(taskId);

        // Deserialize ring object
        Ring ring = mapper.readValue(ringJson, Ring.class);

        WorkerTask worker = new WorkerTask(leaderIp, leaderPort, taskId, operatorType, isFinal, operatorArgs, downstream, stageIdx, outputFile, ring, workerHost);
        worker.runTask();
    }

    public void runTask() throws Exception {
        // Start input listener (TCP)
        startAckServer();
        startInputServer();
        startRetryThread();

        // launch operator subprocess after servers are up
        launchOperator();

        Thread.sleep(500); // wait for python operator to be ready

        // Start stdout reader thread
        // new Thread(this::stdoutReaderLoop).start();

        Thread stdoutThread = new Thread(this::stdoutReaderLoop, "stdout-reader-" + taskId);
        stdoutThread.setDaemon(false);  // Important: don't make it daemon
        stdoutThread.start();

        System.out.println("WorkerTask " + taskId + " started operator " + operator);
        logger.info("Task {} launched operator {}", taskId, operator);

        System.out.println("Task " + taskId + ": started stdout reader thread");
        logger.info("Task {} started stdout reader thread", taskId);

        sendLoadStatusToLeader();
    }

    private void launchOperator() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add("python3");
        cmd.add("-u"); // unbuffered output

        switch (operator) {
            case "filter": cmd.add("/home/anantg2/mp4-g76/src/main/java/utils/operator_filter.py"); break;
            case "transform": cmd.add("/home/anantg2/mp4-g76/src/main/java/utils/operator_transform.py"); break;
            case "aggregate": cmd.add("/home/anantg2/mp4-g76/src/main/java/utils/operator_aggregate.py"); break;
            case "identity": cmd.add("/home/anantg2/mp4-g76/src/main/java/utils/operator_identity.py"); break;
            default:
                System.err.println("Unknown operator: " + operator);
                System.exit(1);
        }

        cmd.add(operatorArgs);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // pb.redirectErrorStream(true);

        operatorProc = pb.start();

        new Thread(() -> {
            try (BufferedReader errReader = new BufferedReader(
                new InputStreamReader(operatorProc.getErrorStream()))) {  // ← ErrorStream, not InputStream!
                String line;
                while ((line = errReader.readLine()) != null) {
                    System.err.println("[Task-" + taskId + " STDERR] " + line);
                }
            } catch (Exception e) {
                logger.debug("Task {} stderr reader closed", taskId);
            }
        }, "stderr-consumer-" + taskId).start();

        // combine python process output
        // new Thread(() -> {
        //     try (BufferedReader br = new BufferedReader(new InputStreamReader(operatorProc.getInputStream()))) {
        //         String line;
        //         while ((line = br.readLine()) != null) {
        //             System.out.println("[PYTHON OUTPUT] " + line);
        //         }
        //     } catch (Exception e) {
        //         e.printStackTrace();
        //     }
        // }).start();

        opStdin = new BufferedWriter(new OutputStreamWriter(operatorProc.getOutputStream()));
        opStdout = new BufferedReader(new InputStreamReader(operatorProc.getInputStream()));
    }

    private void stdoutReaderLoop() {
        try {
            String line;
            boolean written;

            if (opStdout == null) { 
                System.err.println("Task " + taskId + ": opStdout is null!");
                return;
            }

            System.out.println("Task " + taskId + ": starting stdout reader loop");

            while ((line = opStdout.readLine()) != null) {
                // System.out.println("Inside while loop of stdoutReaderLoop of task " + taskId);
                // System.out.println("Reading line: " + line);

                // if final task, write to HyDFS (stubbed here)
                if (isFinal) {
                    // System.out.println(line);
                    
                    // write out to final output file in HyDFS 
                    written = hdfs.appendTuple(OUTPUT_FILE, line);

                    if (!written) {
                        logger.error("Task {} failed to append output tuple {} to output file {}", taskId, line, OUTPUT_FILE);
                    }
                    
                    // for now just write to a local file in hdfs/output.txt using FileOutputStream
                    // fos.write((line + "\n").getBytes());

                } 
                // else, forward to downstream tasks
                else {
                    forwardTuple(line, 0); // initial retry count 0
                }
            }
        } catch (Exception e) {
            System.err.println("Task " + taskId + " stdout reader loop crashed: " + e);
            e.printStackTrace();
        }
    }

    private void forwardTuple(String output, int retryCount) {
        try {
            // parse output JSON
            ObjectNode js = (ObjectNode) mapper.readTree(output);

            // add source task ID
            js.put("srcTask", taskId);

            // extract tuple ID
            String tupleId = js.get("id").asText();
            String newJson = js.toString();

            // add to pending tuples map to track for ACKs
            pendingTuples.put(tupleId, new PendingTuple(tupleId, newJson, retryCount));

            // choose downstream target based on hash of tuple ID
            int idx = hash(tupleId).mod(BigInteger.valueOf(downstream.size())).intValue();
            DownstreamTarget target = downstream.get(idx);

            // Send to exactly one downstream target
            try (Socket socket = new Socket(target.host, target.port);
                BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

                w.write(newJson + "\n");
                w.flush();
            }
        } catch(Exception e){
            System.err.println("Task " + taskId + " failed to forward tuple: " + e);
            logger.error("Failed to forward the tuple from task {}",taskId,e);
            e.printStackTrace();
        }
    }

    private void startInputServer() throws Exception {
        int listenPort = 9000 + taskId; // deterministic mapping

        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(listenPort)) {
                System.out.println("Starting input server for task " + taskId + " listening on port " + listenPort);
                logger.info("Task {} input server started on port {}", taskId, listenPort);

                while (true) {
                    Socket client = server.accept();
                    new Thread(() -> handleClient(client)).start();
                }
            } catch (Exception e) {
                logger.error("Task {} input server crashed", taskId, e);
            }
        }).start();
    }

    private void startRetryThread() {
        Thread retryThread = new Thread(() -> {
            while (true) {
                try {
                    long now = System.currentTimeMillis();
                    for (PendingTuple p : pendingTuples.values()) {
                        if (now - p.lastSentTime >= RETRY_INTERVAL) {
                            retrySend(p);
                        }
                    }
                    Thread.sleep(500);
                } catch (Exception e) {
                    logger.error("Retry thread crashed", e);
                }
            }
        });
        retryThread.setDaemon(true);
        retryThread.start();

        System.out.println("Task " + taskId + ": started retry thread");
        logger.info("Task {} started retry thread", taskId);
    }

    private void retrySend(PendingTuple p) {
        // first check if we have exceeded max retries
        if (p.retryCount >= MAX_RETRIES) {
            // get the downstream target this tuple is supposed to go to
            int idx = hash(p.tupleId).mod(BigInteger.valueOf(downstream.size())).intValue();
            DownstreamTarget target = downstream.get(idx);
            
            // notify leader that the downstream task has likely failed since we couldnt reach it after max retries
            WorkerTaskFailRequest req = new WorkerTaskFailRequest(taskId, target.getTaskId());
            try (Socket socket = new Socket(leaderIp, leaderPort);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                
                out.writeObject(req);
                out.flush();
                logger.info("Task {} notified leader {}:{} of failure of downstream task {} on host {} and port {}", taskId, leaderIp, leaderPort, target.getTaskId(), target.getHost(), target.getPort());
                return; // exit after notifying leader
            } catch (Exception err) {
                logger.error("Task {}: failed to notify leader about failure of downstream task {}", taskId, target.getTaskId(), err);
                return; // exit even if notification fails
            }
        }

        // otherwise proceed with retrying the send
        logger.info("Task {} retrying tuple {} (attempt #{})", taskId, p.tupleId, p.retryCount + 1);

        // try forwarding the tuple again
        forwardTuple(p.json, p.retryCount + 1);

        // we might not even need to update these here since forwardTuple creates a new PendingTuple entry
        // but we do it here to keep the retryCount accurate in case of failures
        p.lastSentTime = System.currentTimeMillis();
        p.retryCount++;
    }

    private void handleClient(Socket client) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(client.getInputStream()));
             FileOutputStream fos = new FileOutputStream("hdfs/" + taskLogPath, true)) {
            String line;
            while ((line = r.readLine()) != null) {
                tuplesCount.incrementAndGet();
                JsonNode js = mapper.readTree(line);
                String tupleId = js.get("id").asText();
                String csvData = js.get("line").asText();
                int upstreamTask = js.get("srcTask").asInt();
                if (seenInputTuples.contains(tupleId)) {
                    logger.debug("Task {} dropping duplicate tuple {}", taskId, tupleId);
                    
                    // ack back to upstream even in case of duplicate
                    // only send ack if upstreamTask is not -1 (source task)
                    if (upstreamTask != -1) {
                        sendAck(upstreamTask, tupleId);
                    }
                    continue;
                }

                // System.out.println("Task " + taskId + ": received tuple " + tupleId + " from upstream task " + upstreamTask);
                // System.out.println(line + "\n");

                // add to seen set and write to the log
                seenInputTuples.add(tupleId);
                fos.write(("INPUT " + tupleId + "\n").getBytes());

                // appendToTaskLog("INPUT " + tupleId);
                // forward to operator stdin
                opStdin.write(csvData + "\n");
                opStdin.flush();

                // only send ack if upstreamTask is not -1 (source task)
                if (upstreamTask != -1) {
                    sendAck(upstreamTask,tupleId);
                }
            }
        }
        catch (Exception e) {
            logger.error("Task {} handleClient error", taskId, e);
            System.out.println("Task " + taskId + " handleClient error: " + e);
        }
    }

    private void sendAck(int upstreamTaskId, String tupleId) {
        try {
            String host = hostOf(upstreamTaskId);
            int port = portOf(upstreamTaskId);

            try (Socket sock = new Socket(host, port);
                 ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream())) {

                out.writeObject(new TupleAck(tupleId));
                out.flush();
            }
        } catch (Exception e) {
            logger.error("Task {} failed to send ACK for {} to upstream {}", taskId, tupleId, upstreamTaskId, e);
            System.out.println("Task " + taskId + " failed to send ACK for " + tupleId + " to upstream " + upstreamTaskId + ": " + e);
        }
    }

    private String hostOf(int taskId){
        if (taskId == -1) return leaderIp; // source task

        return ips.get(taskId%ips.size());
    }

    private int portOf(int taskId){
        if (taskId == -1) return leaderPort; // source task

        return 10000 + taskId;
    }

    private boolean rebuildStateFromLog() {
        try {
            String hdfsName = taskLogPath;

            // download log file from HyDFS to local
            String localName = "task_" + taskId + "_log_local.txt";

            boolean ok = hdfs.getHyDFSFileToLocalFileFromOwner(hdfsName, localName);
            if (!ok) {
                logger.info("No prior log found for task {}", taskId);
                return false;
            }
            List<String> lines = Files.readAllLines(Paths.get("output/" + localName));
            for (String line : lines) {
                if (line.startsWith("INPUT ")) {
                    String tupleId = line.split(" ")[1];
                    seenInputTuples.add(tupleId);
                }
            }
            logger.info("Task {} rebuilt state: {} tuples", taskId, seenInputTuples.size());

            return true;

        } catch (Exception e) {
            logger.error("Task {} failed to rebuild state", taskId, e);
            return false;
        }
    }

    private void appendToTaskLog(String logLine) {
        try {
            // append to the tasks log file in HyDFS
            hdfs.appendTuple(taskLogPath, logLine + "\n");
        } catch (Exception e) {
            logger.error("Task {}: failed to append to HyDFS log {}", taskId, taskLogPath, e);
        }
    }

    private void startAckServer() {
        int ackPort = 10000 + taskId;
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(ackPort)) {
                System.out.println("Task " + taskId + " ACK server listening on port " + ackPort);
                logger.info("Task {} ACK server started on port {}", taskId, ackPort);

                while (true) {
                    Socket client = server.accept();
                    new Thread(() -> handleAck(client)).start();
                }
            } catch (Exception e) {
                logger.error("Task {} ACK server crashed", taskId, e);
            }
        }).start();
    }

    private void handleAck(Socket client) {
        try (ObjectInputStream in = new ObjectInputStream(client.getInputStream())) {
            TupleAck ack = (TupleAck) in.readObject();
            String tupleId = ack.getTupleId();
            pendingTuples.remove(tupleId);
            logger.debug("Task {} received ACK for tuple {}, removing from pending tuples map", taskId, tupleId);
        } catch (Exception e) {
            logger.error("Task {} failed to handle ACK", taskId, e);
        }
    }

    private void sendLoadStatusToLeader() {
        Thread reporter = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    long count = tuplesCount.getAndSet(0);
                    WorkerTaskLoad report = new WorkerTaskLoad(taskId, stageIdx, count);
                    sendLoadStatus(report);
                } catch (Exception e) {
                    logger.error("Task {} load reporter crashed", taskId, e);
                }
            }
        });
        reporter.setDaemon(true);
        reporter.start();

        System.out.println("Task " + taskId + ": started load reporter thread");
        logger.info("Task {} started load reporter thread", taskId);
    }

    private void sendLoadStatus(WorkerTaskLoad rep) {
        try (Socket sock = new Socket(leaderIp, leaderPort);
             ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream())) {
            out.writeObject(rep);
            out.flush();
        } catch (Exception e) {
            logger.error("Task {} failed sending load report to leader {}:{}", taskId, leaderIp, leaderPort, e);
        }
    }

    // hash function using SHA-1 consistent hashing. Returns the hash as a BigInteger so its easy to compare
    private BigInteger hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] bytes = md.digest(key.getBytes());
            return new BigInteger(1, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Error computing SHA-1 hash", e);
        }
    }
}

// Stub for fetching routing info
// get list of subsequent tasks from leader for this taskId
// eg. task 2 -> tasks 5,6,7
class RoutingLoader {
    public static List<DownstreamTarget> load(int taskId) {
        // leader will place routing files on VM
        List<DownstreamTarget> targets = new ArrayList<>();
        //Make sure this routing file path matches with what the server wrote to
        String filename = "routing/routing_" + taskId + ".conf";

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

        System.out.println("RoutingLoader: task " + taskId + " downstream targets size of: " + targets.size());
        return targets;
    }
}