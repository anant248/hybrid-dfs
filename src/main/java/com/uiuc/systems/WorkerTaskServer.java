package com.uiuc.systems;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WorkerTaskServer implements Runnable{
    private static final int PORT = 7979;
    private volatile boolean running = true;
    private ServerSocket sc;

    private static final Map<Integer, Process> taskProcessMap = new ConcurrentHashMap<>();

    public WorkerTaskServer(){}

    @Override
    public void run(){
        try {
            sc=new ServerSocket(PORT);
            sc.setReuseAddress(true);
            System.out.println("Worker Task Server listening on port " + PORT);

            while(running){
                Socket socket = sc.accept();
                new Thread(() -> handle(socket)).start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeServerSocket();
        }
    }

    private static void handle(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        ) {
            //TODO: DO WE HAVE TO FLUSH INITIALLY TO EXCHANGE HEADERS?
            out.flush();
            Object obj = in.readObject();
            if (obj instanceof WorkerTaskRoutingFileRequest) {
                Files.createDirectories(Paths.get("routing"));
                String path = "routing/routing_" + ((WorkerTaskRoutingFileRequest) obj).getTaskId() + ".conf";
                Files.writeString(Paths.get(path), ((WorkerTaskRoutingFileRequest) obj).getFileContent());
                System.out.println("Routing file created at: " + path);
            }

            else if (obj instanceof StartWorkerTaskRequest) {
                List<String> cmd = new ArrayList<>();
                cmd.add("java");
                cmd.add("-cp");
                cmd.add("/home/anantg2/mp4-g76/target/HybridDistributedFileSystem-1.0-SNAPSHOT.jar");
                cmd.add("com.uiuc.systems.WorkerTask");
                cmd.add(((StartWorkerTaskRequest) obj).getLeaderIp());
                Integer port = ((StartWorkerTaskRequest) obj).getLeaderPort();
                cmd.add(port.toString());
                cmd.add(Integer.toString(((StartWorkerTaskRequest) obj).getTaskId()));
                cmd.add(Integer.toString(((StartWorkerTaskRequest) obj).getStageIdx()));
                cmd.add(((StartWorkerTaskRequest) obj).getOperator());
                cmd.add(((StartWorkerTaskRequest) obj).isFinal() ? "1" : "0");
                cmd.add(((StartWorkerTaskRequest) obj).getOutputFileName());

                // serialize ring object to JSON string
                Ring ring = ((StartWorkerTaskRequest) obj).getRing();
                ObjectMapper mapper = new ObjectMapper();
                mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
                String ringJson = mapper.writeValueAsString(ring);

                cmd.add(ringJson);
                cmd.add(((StartWorkerTaskRequest) obj).getWorkerHost());
                cmd.add(((StartWorkerTaskRequest) obj).getOperatorArgs());

                System.out.println("Starting WorkerTask with command: " + String.join(" ", cmd));

                Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
 
                // new Thread(() -> {
                //     try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                //         String line;
                //         while ((line = br.readLine()) != null) {
                //             System.out.println("[WorkerTask OUTPUT] " + line);
                //         }
                //     } catch (Exception e) {
                //         e.printStackTrace();
                //     }

                // }).start();


                System.out.println("process started with PID=" + p.pid());

                int taskId = ((StartWorkerTaskRequest) obj).getTaskId();
                taskProcessMap.put(taskId, p);
                System.out.println("Started WorkerTask: " + ((StartWorkerTaskRequest) obj).getTaskId());
            }

            else if (obj instanceof KillWorkerTask){
                int taskId = ((KillWorkerTask) obj).getTaskId();
                Process p = taskProcessMap.get(taskId);
                boolean killed = false;
                if (p != null) {
                    p.destroyForcibly();
                    killed = true;
                    taskProcessMap.remove(taskId);
                    System.out.println("Killed WorkerTask " + taskId + " (PID=" + p.pid() + ")");
                    WorkerLoggerHelper.taskEnd(taskId, "KILLED_BY_WORKER_SERVER");
                } else {
                    System.out.println("No running process found for WorkerTask " + taskId);
                }

                out.writeObject(new WorkerTaskKillAckRequest(taskId,killed));
                out.flush();
            }

            else if (obj instanceof GetPidRequest) {
                int taskId = ((GetPidRequest) obj).getTaskId();
                long pid = -1L;
                Process p = taskProcessMap.get(taskId);
                if (p != null) {
                    try {
                        pid = p.pid();
                    } catch (Throwable ignored) { pid = -1L; }
                }
                out.writeObject(new GetPidResponse(taskId, pid));
                out.flush();
            }
            else if (obj instanceof FetchLogRequest) {
                FetchLogRequest req = (FetchLogRequest) obj;

                String path = "hdfs/rainstorm_task_" + req.taskId + ".log";
                List<String> lines = Collections.emptyList();

                try {
                    File file = new File(path);
                    if (file.exists()) {
                        lines = Files.readAllLines(file.toPath());
                        //DELETE THE LOG HERE
                        boolean deleted = file.delete();
                        if (!deleted) {
                            System.err.println("FetchLog handle: failed to delete old log " + path);
                        } else {
                            System.out.println("FetchLog handle: deleted old log " + path);
                        }
                    }
                } catch (Exception ignored) {}

                try {
                    out.writeObject(new FetchLogResponse(req.taskId, lines));
                    out.flush();
                } catch (Exception e) {
                    System.err.println("FetchLog: failed sending FetchLogResponse for task " + req.taskId + " : " + e);
                }
            }

            else if (obj instanceof InstallLog) {
                InstallLog req = (InstallLog) obj;
                try {
//                    File dir = new File("restore_log");
//                    dir.mkdirs();
                    String logsPath = "hdfs/rainstorm_task_" + req.taskId + ".log";
                    Files.write(Paths.get(logsPath), req.lines);
                    out.writeObject("OK");
                    out.flush();
                    System.out.println("WorkerTaskServer: installed restore log at " + logsPath);
                } catch (Exception e) {
                    System.err.println("WorkerTaskServer: failed to store restore log for task "
                            + req.taskId + " : " + e);

                    out.writeObject("ERROR");
                    out.flush();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Helper to retrieve the process ID associated with the task ID, call this in Main.java
    public static Long getPidForTask(int taskId) {
        return taskProcessMap.get(taskId).pid();
    }

    public void stopServer() {
        running = false;
        closeServerSocket();
        System.out.println("Worker Task server stopped.");
    }

    private void closeServerSocket() {
        try {
            if (sc != null && !sc.isClosed()) sc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
