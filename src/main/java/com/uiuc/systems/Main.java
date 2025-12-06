package com.uiuc.systems;

import java.util.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.file.*;

import com.uiuc.systems.NodeInfo.State;

public class Main {
    private static final int PORT = 6971;
    private static final long PROTOCOL_INTERVAL = 2000;
    // keep a reference to the currently running RainStormLeader (if any)
    private static RainStormLeader currentRainStormLeader = null;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: java src/main/java/com/uiuc/systems/Main.java <current node Id> <introducer node Id> <gossip/ping> <suspect/nonsuspect>");
            System.exit(1);
        }

        // get current node id and introducer id from initial startup
        String id = args[0]; // e.g. "127.0.0.1"
        String introducerId = args[1]; // eg. "128.0.0.2"
        String mode = args[2];
        String suspicionMode = args[3];
        boolean suspicionFlag = (suspicionMode.equalsIgnoreCase("suspect")) ? true : false;

        // build 2 x NodeId (one for current and one for introducer)
        // build 2 x NodeInfo (one for current and one for introducer)
        // build membershiplist cosisting of these two <NodeId, NodeInfo>
        NodeId selfNode = new NodeId(id, PORT, System.currentTimeMillis());
        NodeId introNode = new NodeId(introducerId, PORT, System.currentTimeMillis());

        NodeInfo selfInfo = new NodeInfo(NodeInfo.State.ALIVE, System.currentTimeMillis(), 0L);
        NodeInfo introInfo = new NodeInfo(NodeInfo.State.ALIVE, System.currentTimeMillis(), 0L);

        // build ring instance
        Ring ring = new Ring();

        // clean-up any old files in hydfs and output folders
        cleanDirectory("hdfs");
        cleanDirectory("output");

        // build HyDFS instance
        HyDFS hydfs = new HyDFS(selfNode, ring);

        // set global HyDFS instance
        GlobalHyDFS globalHyDFS = new GlobalHyDFS();
        globalHyDFS.setHdfs(hydfs);

        // build membership list instance
        MemberShipList membershipList = new MemberShipList(ring, hydfs);

        if (id.equals(introducerId)) {
            membershipList.addNodeEntry(selfNode, selfInfo);
        } 
        else { 
            membershipList.addNodeEntry(selfNode, selfInfo);
            membershipList.addNodeEntry(introNode, introInfo);
        }

        // build Cassandra style virtual ring
        ring.updateRing(membershipList);

        // build message
        Message message = new Message(selfNode, membershipList.getMemberShipList());
        // set default message params
        message.setProtocol(mode);
        
        if (!suspicionFlag) {
            message.disableSuspicionMode();
        }
        else {
            message.enableSuspicionMode();
        }

        message.setDropRate(0.0);

        // build UDP Client
        UDPClient client = new UDPClient();

        // build Gossip
        // build PingAck
        Gossip gossipProtocol = new Gossip(client, membershipList, selfNode, ring, hydfs);
        PingAck pingProtocol = new PingAck(client, membershipList, selfNode);

        // build UDP Server
        UDPServer server = new UDPServer(gossipProtocol, pingProtocol, message.getSuspicionMode(), message.getProtocol());

        // build HyDFS Server
        HyDFSServer hyDFSServer = new HyDFSServer(hydfs);

        // start hyDFSServer
        new Thread(hyDFSServer).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("JVM shutting down — stopping HyDFSServer...");
            hyDFSServer.stopServer();
        }));

        // start WorkerTaskServer if this node is not the introducer (rainstorm leader)
        if (!id.equals(introducerId)) {
            System.out.println("Starting WorkerTaskServer on non-introducer node...");

            WorkerTaskServer workerTaskServer = new WorkerTaskServer();
            new Thread(workerTaskServer).start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("JVM shutting down — stopping WorkerTaskServer...");
                workerTaskServer.stopServer();
            }));
        }

        // build MembershipScheduler
        MemberShipScheduler msScheduler = new MemberShipScheduler();

        // build ProtocolManager
        ProtocolManager protocolManager = new ProtocolManager(gossipProtocol, pingProtocol, msScheduler, PROTOCOL_INTERVAL, client, server);

        // start UDP server
        protocolManager.startUDPServer(() -> server.start());

        // start PingAck by default
        if (mode.equalsIgnoreCase("gossip")) {
            protocolManager.startProtocol(() -> gossipProtocol.sendGossip(suspicionFlag));
        }
        else {
            protocolManager.startProtocol(() -> pingProtocol.sendPing(suspicionFlag));
        }


        System.out.println("Node started with id=" + id + ", introducer=" + introducerId);
        System.out.println("Commands:");
        System.out.println("  list_mem                  -> list membership list");
        System.out.println("  list_self                 -> display this node's info");
        System.out.println("  join                      -> join the group");
        System.out.println("  leave                     -> voluntarily leave the group");
        System.out.println("  display_suspects          -> display suspected nodes");
        System.out.println("  ls HyDFSfilename          -> list VM address and VM hash where this file is being stored, along with the file hash");
        System.out.println("  liststore                 -> list filename and filename hash that this VM is storing, along with VM hash");
        System.out.println("  list_mem_ids              -> list of nodes on ring, sorted by nodeId");
        System.out.println("  drop <0.0 ... 1.0>        -> induce the packet drop rate");
        System.out.println("  display_protocol          -> display current protocol and suspicion status");
        System.out.println("  quit                      -> exit");
        System.out.println("  merge HyDFSfilename       -> ensures that all replicas of a file are identical");
        System.out.println("  create localfilename HyDFSfilename -> create a file on HyDFS and copy the contents of localfilename from local dir");
        System.out.println("  get HyDFSfilename localfilename    -> fetches the entire file from HyDFS to localfilename on local dir");
        System.out.println("  append localfilename HyDFSfilename -> appends the contents of the localfilename to the end of the file on HyDFS");
        System.out.println("  getfromreplica VMaddress HyDFSfilename localfilename                 -> get the HyDFSfilename from a particular replica and store it locally as localfilename");
        System.out.println("  multiappend HyDFSfilename VMi ... VMj localfilenamei localfilenamej -> Launches appends from VMi,…VMj simultaneously to HyDFSfilename; VMi appends the contents of localfilenamei");
        System.out.println("  switch <protocol> <suspect/nosuspect>                                -> switch protocol and suspicion mode");
        System.out.println("  rainstorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> … <opNstages_exe> <opNstages_args> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>  -> start a RainStorm job (only on introducer node)");
        System.out.println("  list_tasks                          -> list all current RainStorm tasks (only on introducer node)");
        System.out.println("  kill_task <VM> <Task Id> <PID>      -> kill a specific RainStorm task (only on introducer node)");

        // listen for terminal input
        Scanner sc = new Scanner(System.in);
        while (true) {
            String line = sc.nextLine();
            if (line == null) continue;
            line = line.trim();
            String lowerLine = line.toLowerCase();

            if (lowerLine.equals("quit")) {
                protocolManager.stopProtocol();
                break;
            } else if (lowerLine.startsWith("create ")) {
                // create localfilename HyDFSfilename
                String[] parts = line.split("\\s+");
                if (parts.length != 3) {
                    System.out.println("Usage: create <localfilename> <HyDFSfilename>");
                    continue;
                }
                String localFile = parts[1];
                String hdfsFile = parts[2];
                boolean ack = hydfs.sendCreateRequestToOwner(localFile, hdfsFile);
                if (ack) {
                    System.out.println("ACK: File " + hdfsFile + " created in HyDFS.");
                } else {
                    System.out.println("NACK: Failed to create file " + hdfsFile + " in HyDFS.");
                }
            } else if (lowerLine.startsWith("batchcreate")) {
                // batchcreate <count> <size>
                String[] parts = line.split("\\s+");
                if (parts.length != 3) {
                    System.out.println("Usage: batchcreate <count> <size>");
                    System.out.println("Example: batchcreate 100 1K");
                    continue;
                }

                int count;
                try {
                    count = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid count. Must be an integer.");
                    continue;
                }
                
                String sizeLabel = parts[2]; // e.g. "1K", "10K", etc.
                
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < count; i++) {
                    String localFile = "file_" + sizeLabel + "_" + i + ".txt";
                    String hdfsFile = "hdfsfile_" + sizeLabel + "_" + i + ".txt";
                    boolean verbose = count <= 20;

                    boolean ack = hydfs.sendCreateRequestToOwner(localFile, hdfsFile);
                    if (verbose) {
                        System.out.println("ACK: Created " + hdfsFile);
                    } else if (!ack) {
                        System.out.println("NACK: Failed to create " + hdfsFile);
                    }
                }

                long endTime = System.currentTimeMillis();
                System.out.println("Batch create of " + count + " files (" + sizeLabel + ") completed in " 
                                + (endTime - startTime) + " ms.");
            } else if (lowerLine.startsWith("get ")) {
                // get HyDFSfilename localfilename
                String[] parts = line.split("\\s+");
                if (parts.length != 3) {
                    System.out.println("Usage: get <HyDFSfilename> <localfilename>");
                    continue;
                }
                String hdfsFile = parts[1];
                String localFile = parts[2];
                boolean ack = hydfs.getHyDFSFileToLocalFileFromOwner(hdfsFile, localFile);
                if (ack) {
                    System.out.println("ACK: File " + hdfsFile + " retrieved from HyDFS and saved as " + localFile);
                } else {
                    System.out.println("NACK: Failed to retrieve file " + hdfsFile + " from HyDFS.");
                }
            } else if (lowerLine.startsWith("append ")) {
                // append localfilename HyDFSfilename
                String[] parts = line.split("\\s+");
                if (parts.length != 3) {
                    System.out.println("Usage: append <localfilename> <HyDFSfilename>");
                    continue;
                }
                String localFile = parts[1];
                String hdfsFile = parts[2];
                boolean ack = hydfs.sendAppendRequestToOwner(localFile, hdfsFile);
                if (ack) {
                    System.out.println("ACK: Appended " + localFile + " to " + hdfsFile + " in HyDFS.");
                } else {
                    System.out.println("NACK: Failed to append " + localFile + " to " + hdfsFile + " in HyDFS.");
                }
            } else if (lowerLine.startsWith("merge ")) {
                // merge HyDFSfilename
                String[] parts = line.split("\\s+");
                if (parts.length != 2) {
                    System.out.println("Usage: merge <HyDFSfilename>");
                    continue;
                }
                String hdfsFile = parts[1];
                hydfs.sendMergeRequestToOwner(hdfsFile);
            } else if (lowerLine.equals("list_mem")) {
                for (Map.Entry<NodeId, NodeInfo> entry : membershipList.getMemberShipList().entrySet()) {
                    NodeId node = entry.getKey();
                    System.out.println("IP: " + node.getIp() + " | Port: " + node.getPort() + " | Version: " + node.getVersion());
                }

            } else if (lowerLine.equals("list_self")) {
                System.out.println("IP: " + selfNode.getIp() + " | Port: " + selfNode.getPort() + " | Version: " + selfNode.getVersion());

            } else if (lowerLine.equals("join")) {
                // build 2 x NodeId (one for current and one for introducer)
                // build 2 x NodeInfo (one for current and one for introducer)
                // build membershiplist cosisting of these two <NodeId, NodeInfo>
                NodeId joineeNode = new NodeId(id, PORT, System.currentTimeMillis());
                NodeId newIntroNode = new NodeId(introducerId, PORT, System.currentTimeMillis());

                NodeInfo joineeInfo = new NodeInfo(NodeInfo.State.ALIVE, System.currentTimeMillis(), 0L);
                NodeInfo newIntroInfo = new NodeInfo(NodeInfo.State.ALIVE, System.currentTimeMillis(), 0L);

                membershipList.addNodeEntry(joineeNode, joineeInfo);
                membershipList.addNodeEntry(newIntroNode, newIntroInfo);

                // start the protocol that is currently running
                protocolManager.startProtocol(() -> pingProtocol.sendPing(protocolManager.getSuspicionMode()));
                
            } else if (lowerLine.equals("leave")) {
                // set state to LEAVE
                selfInfo.setState(State.LEAVE);

                // wait for a few seconds then clear the current membership list then stop the portocol (server is still running)
                membershipList.getMemberShipList().clear();
                protocolManager.stopProtocol();
                server.stop();

            } else if (lowerLine.equals("display_suspects")) {
                System.out.println(membershipList.displaySuspects());
            } else if (lowerLine.startsWith("switch")) {
                String[] parts = line.split("\\s+");
                if (parts.length == 3) {
                    if (parts[1].equalsIgnoreCase("gossip") || parts[1].equalsIgnoreCase("ping")) {
                        message.setProtocol(parts[1]);
                    }
                    else {
                        System.out.println("Typo in gossip/ping, please retype");
                        continue;
                    }

                    if (parts[2].equalsIgnoreCase("suspect")) {
                        message.enableSuspicionMode();
                    } 
                    else if (parts[2].equalsIgnoreCase("nosuspect")) {
                        message.disableSuspicionMode();
                    }
                    else {
                        System.out.println("Typo in suspect/nosuspect, please retype");
                        continue;
                    }

                    protocolManager.switchProtocol(message.getProtocol(), message.getSuspicionMode());
                } else {
                    System.out.println("Usage: switch <protocol> <suspect/nosuspect>");
                }
            } else if (lowerLine.equals("display_protocol")) {
                System.out.println(message.displayProtocol());
            } else if (lowerLine.startsWith("drop")) {
               try {
                   double p = Double.parseDouble(line.split(" ")[1]);
                   server.setDropRate(p);
               } catch (Exception e) {
                   System.out.println("Usage: drop <0.0 .. 1.0>");
               }
            } else if (lowerLine.startsWith("ls")) {
                String[] parts = line.split("\\s+");
                if (parts.length != 2) {
                    System.out.println("Usage: ls <HyDFSfilename>");
                    continue;
                }
                String filename = parts[1];
                List<NodeId> replicas = hydfs.verifyExistingReplicas(ring.getReplicas(filename), filename);
                replicas.sort(Comparator.comparing(e -> ring.getNodeHash(e)));
                BigInteger fileHash = ring.hash(filename);

                // print filename | file ID
                if (!replicas.isEmpty()) {
                    System.out.println(filename + " | File ID: " + fileHash);
                }
                else {
                    System.out.println("No such file exists across HyDFS!");
                }
                for (NodeId node : replicas) {
                    System.out.println("IP: " + node.getIp() + " | Port: " + node.getPort() + " | Version: " + node.getVersion() + " | Ring ID: " + ring.getNodeHash(node));
                }
            } else if (lowerLine.equals("liststore")) {
                // print the node IP | node ID
                System.out.println(selfNode.getIp() + " | Node Ring ID: " + ring.getNodeHash(selfNode));

                // retrieve HyDFS files from this node by getting access to its file meta map keys, print the file | file ID
                for (String file : hydfs.getFileMetaMap().keySet()) {
                    System.out.println(file + " | FileID: " + ring.hash(file));
                }
            } else if (lowerLine.startsWith("getfromreplica")) {
                String[] parts = line.split("\\s+");
                if (parts.length != 4) {
                    System.out.println("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>");
                    continue;
                }
                String vmAddress = parts[1];
                String dfsFile = parts[2];
                String localFile = parts[3];

                boolean ack = hydfs.getHyDFSFileToLocalFileFromReplica(vmAddress, dfsFile, localFile);
                if (ack) {
                    System.out.println("ACK: File " + dfsFile + " retrieved from HyDFS at " + vmAddress + " and saved as " + localFile);
                } else {
                    System.out.println("NACK: Failed to retrieve file " + dfsFile + " from HyDFS at " + vmAddress);
                }
            } else if (lowerLine.equals("list_mem_ids")) {
                List<Map.Entry<NodeId, NodeInfo>> sortedEntries = new ArrayList<>(membershipList.getMemberShipList().entrySet());
                sortedEntries.sort(Comparator.comparing(e -> ring.getNodeHash(e.getKey())));

                for (Map.Entry<NodeId, NodeInfo> entry : sortedEntries) {
                    NodeId node = entry.getKey();
                    System.out.println("IP: " + node.getIp() + " | Port: " + node.getPort() + " | Version: " + node.getVersion() + " | Ring ID: " + ring.getNodeHash(node));
                }
            } else if (lowerLine.startsWith("multiappend")) {
                String[] multiAppendArray = lowerLine.split("\\s+");
                if (multiAppendArray.length<6 || (multiAppendArray.length-2)%2!=0) {
                    System.out.println("multi-append command requires at least 2 client IPs,2 local filenames and hdfs filename");
                    continue;
                }

                String hdfsFileName = multiAppendArray[1];
                int numberOfVms = (multiAppendArray.length-2)/2;
                String[] clientIps = Arrays.copyOfRange(multiAppendArray,2,2+numberOfVms);
                String[] localFiles = Arrays.copyOfRange(multiAppendArray,2+numberOfVms,multiAppendArray.length);

                hydfs.sendMultiAppendRequestToNodes(clientIps,localFiles,hdfsFileName);
            } else if (lowerLine.startsWith("ring")) {
                ring.printRing();
            } else if (lowerLine.startsWith("meta")) {
                hydfs.printFileMetaMap(); 
            }
            else if (lowerLine.startsWith("rainstorm")) {
                System.out.println("RainStorm leader invocation received.");

                String[] parts = line.split("\\s+");
                if (parts.length < 9) {
                    System.out.println("Usage: RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> … <opNstages_exe> <opNstages_args> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>");
                    continue;
                }

                try {
                    int idx = 1;
                    int Nstages = Integer.parseInt(parts[idx++]);
                    int NtasksPerStage = Integer.parseInt(parts[idx++]);

                    List<String> operatorsAndArgs = new ArrayList<>();
                    for (int s = 0; s < Nstages; s++) {
                        if (idx + 1 >= parts.length) {
                            System.out.println("Error: insufficient operator/args entries.");
                            continue;
                        }
                        operatorsAndArgs.add(parts[idx++]); 
                        operatorsAndArgs.add(parts[idx++]); 
                    }

                    String hydfsSrc = parts[idx++];
                    String hydfsDest = parts[idx++];
                    boolean exactlyOnce = Boolean.parseBoolean(parts[idx++]);
                    boolean autoscale = Boolean.parseBoolean(parts[idx++]);
                    int inputRate = Integer.parseInt(parts[idx++]);
                    int lw = Integer.parseInt(parts[idx++]);
                    int hw = Integer.parseInt(parts[idx++]);

                    RainStormLeader rainStormLeader = new RainStormLeader(
                        selfNode.getIp(),
                        Nstages,
                        NtasksPerStage,
                        exactlyOnce,
                        autoscale,
                        inputRate,
                        lw,
                        hw,
                        operatorsAndArgs,
                        hydfsSrc,
                        hydfsDest,
                        ring
                    );

                    // keep global reference so Main can query it later
                    currentRainStormLeader = rainStormLeader;

                    new Thread(() -> {
                        try {
                            rainStormLeader.run();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }).start();

                } catch (Exception e) {
                    System.out.println("Error parsing RainStorm arguments: " + e.getMessage());
                }
            }
            else if (lowerLine.equals("list_tasks")) {
                System.out.println("Listing RainStorm tasks...");

                printRainStormTasks();

            }
            else if (lowerLine.startsWith("kill_task")) {
                String[] parts = line.split("\\s+");
                if (parts.length != 4) {
                    System.out.println("Usage: kill_task <VM> <Task ID> <PID>");
                } else {
                    String vm = parts[1];
                    int taskId = Integer.parseInt(parts[2]);
                    int pid = Integer.parseInt(parts[3]);
                    try {

                        new Thread(() -> {
                            try {
                                currentRainStormLeader.killTask(vm, taskId, pid);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }).start();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("Unknown command: " + line);
            }
        }
    }

    private static void cleanDirectory(String dirPath) {
        Path dir = Paths.get(dirPath);
        if (Files.exists(dir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path file : stream) {
                    Files.deleteIfExists(file);
                }
            } catch (Exception e) {
                System.err.println("Error cleaning directory " + dirPath + ": " + e.getMessage());
            }
        } else {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                System.err.println("Could not create directory " + dirPath);
            }
        }
    }

    // helper method to print out all current tasks across all stages
    private static void printRainStormTasks() {

        // in case currentRainStormLeader is null
        if (currentRainStormLeader == null) {
            System.out.println("No running RainStorm leader found. Start one with the 'rainstorm' command first.");
            return;
        }

        // retrieve tasks map from currentRainStormLeader
        Map<Integer, RainStormLeader.TaskInfo> tasks = currentRainStormLeader.getTasks();
        if (tasks.isEmpty()) {
            System.out.println("No tasks registered in leader.");
            return;
        }

        // header
        System.out.printf("%-8s | %-31s | %-8s | %-6s | %-15s | %-20s%n",
                "TASKID", "VM", "PID", "STAGE", "OPERATOR", "LOGFILE");
        System.out.println("---------------------------------------------------------------------------------------");

        for (Map.Entry<Integer, RainStormLeader.TaskInfo> e : tasks.entrySet()) {
            RainStormLeader.TaskInfo ti = e.getValue();
            int taskId = ti.globalTaskId;
            String host = ti.host;
            int stageIdx = ti.stageIdx;
            String opExe = currentRainStormLeader.getStageOperator(stageIdx);
            String logFile = "hdfs/rainstorm_task_" + taskId + ".log";

            long pid = -1L;
            // RPC to worker's WorkerTaskServer to request PID for taskId
            try (Socket s = new Socket(host, RainStormLeader.WORKER_PORT);
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                out.flush(); // match WorkerTaskServer's pattern
                GetPidRequest req = new GetPidRequest(taskId);
                out.writeObject(req);
                out.flush();

                Object resp = in.readObject();
                if (resp instanceof GetPidResponse) {
                    pid = ((GetPidResponse) resp).getPid();
                } else {
                    pid = -1L;
                }
            } catch (Exception ex) {
                // couldn't contact worker or other error, leave pid as -1
                pid = -1L;
            }

            String pidStr = (pid == -1L) ? "N/A" : Long.toString(pid);
            System.out.printf("%-8d | %-12s | %-8s | %-6d | %-15s | %-20s%n",
                    taskId, host, pidStr, stageIdx, (opExe == null ? "?" : opExe), logFile);
        }
    }
}