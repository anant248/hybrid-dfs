package com.uiuc.systems;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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

            if (obj instanceof StartWorkerTaskRequest) {
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
                cmd.addAll(((StartWorkerTaskRequest) obj).getOperatorArgs());

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

            if (obj instanceof KillWorkerTask){
                int taskId = ((KillWorkerTask) obj).getTaskId();
                Process p = taskProcessMap.get(taskId);
                boolean killed = false;
                if (p != null) {
                    p.destroyForcibly();
                    killed = true;
                    taskProcessMap.remove(taskId);
                    System.out.println("Killed WorkerTask " + taskId + " (PID=" + p.pid() + ")");
                } else {
                    System.out.println("No running process found for WorkerTask " + taskId);
                }

                out.writeObject(new WorkerTaskKillAckRequest(taskId,killed));
                out.flush();
            }

            if (obj instanceof GetPidRequest) {
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
