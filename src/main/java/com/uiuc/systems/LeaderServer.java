package com.uiuc.systems;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class LeaderServer implements Runnable {

    private final RainStormLeader leader;
    private final int port;
    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public LeaderServer(RainStormLeader leader, int port) {
        this.leader = leader;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            System.out.println("LeaderControlServer listening on port " + port);

            while (running) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handle(socket)).start();
            }
        } catch (Exception e) {
            System.err.println("LeaderControlServer crashed: " + e);
        } finally {
            closeServerSocket();
        }
    }

    private void handle(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());) {
            out.flush();
            Object obj = in.readObject();

            if (obj instanceof WorkerTaskFailRequest) {
                WorkerTaskFailRequest req = (WorkerTaskFailRequest) obj;
                System.out.println("Leader received WorkerTaskFailRequest for task: " + req.getFailedTaskId());
                leader.workerTaskFailureHandler(req);
            } else if (obj instanceof WorkerTaskLoad) {
                leader.processWorkerTaskLoad((WorkerTaskLoad) obj);
            } else if (obj instanceof FinalTuple) {
                leader.handleFinalTuple((FinalTuple) obj);
            }
            else if (obj instanceof TaskLogMessage) {
//                leader.handleTaskLog((TaskLogMessage) obj);
            }else if (obj instanceof LoadStateRequest) {
                LoadStateRequest req = (LoadStateRequest) obj;
                leader.handleLoadState(req, out);
            } else if(obj instanceof WorkerLogBatch){
                WorkerLogBatch b = (WorkerLogBatch) obj;
//                String file = "rainstorm_task_" + b.getTaskId() + ".log";
//
//                String combined = String.join("\n", b.getLines()) + "\n";
                leader.handleTaskLog(b);
            }
        } catch (Exception e) {
            System.err.println("Leader failed to handle request: " + e);
        }
    }

    public void stopServer() {
        running = false;
        closeServerSocket();
        System.out.println("LeaderControlServer stopped.");
    }

    private void closeServerSocket() {
        try {
            if (serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
        } catch (Exception ignored) {}
    }
}
