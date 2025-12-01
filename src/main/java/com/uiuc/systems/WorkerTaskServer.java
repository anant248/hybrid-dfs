package com.uiuc.systems;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerTaskServer implements Runnable{
    private static final int PORT = 7979;
    private volatile boolean running = true;
    private ServerSocket sc;

    private static final Map<Integer, Long> taskPidMap = new ConcurrentHashMap<>();

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
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Object obj = in.readObject();
            if (obj instanceof WorkerTaskRoutingFileRequest) {
                String path = "/tmp/routing_" + ((WorkerTaskRoutingFileRequest) obj).getTaskId() + ".conf";
                Files.writeString(Paths.get(path), ((WorkerTaskRoutingFileRequest) obj).getFileContent());
                System.out.println("Routing file created at: " + path);
            }

            if (obj instanceof StartWorkerTaskRequest) {
                List<String> cmd = new ArrayList<>();
                cmd.add("java");
                cmd.add("-cp");
                cmd.add("HybridDistributedFileSystem.jar");
                cmd.add("com.uiuc.systems.WorkerTask");
                cmd.add(((StartWorkerTaskRequest) obj).getLeaderIp());
                Integer port = ((StartWorkerTaskRequest) obj).getLeaderPort();
                cmd.add(port.toString());
                cmd.add(Integer.toString(((StartWorkerTaskRequest) obj).getTaskId()));
                cmd.add(Integer.toString(((StartWorkerTaskRequest) obj).getStageIdx()));
                cmd.add(((StartWorkerTaskRequest) obj).getOperator());
                cmd.add(((StartWorkerTaskRequest) obj).isFinal() ? "1" : "0");
                cmd.addAll(((StartWorkerTaskRequest) obj).getOperatorArgs());

                Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
                long pid = p.pid();
                int taskId = ((StartWorkerTaskRequest) obj).getTaskId();
                taskPidMap.put(taskId, pid);
                System.out.println("Started WorkerTask: " + ((StartWorkerTaskRequest) obj).getTaskId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Helper to retrieve the process ID associated with the task ID, call this in Main.java
    public static Long getPidForTask(int taskId) {
        return taskPidMap.get(taskId);
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
