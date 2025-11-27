package com.uiuc.systems;

import java.io.*;
import java.net.*;
import java.util.*;

public class WorkerTask {

    private final int taskId;
    private final String operator;
    private final List<String> operatorArgs;
    private final List<DownstreamTarget> downstream = new ArrayList<>();

    private Process operatorProc;
    private BufferedWriter opStdin;
    private BufferedReader opStdout;

    public WorkerTask(int taskId, String operator, List<String> operatorArgs, List<DownstreamTarget> downstream) {
        this.taskId = taskId;
        this.operator = operator;
        this.operatorArgs = operatorArgs;
        this.downstream.addAll(downstream);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WorkerTask <taskId> <operatorType> <operatorArgs...>");
            return;
        }

        int taskId = Integer.parseInt(args[0]);
        String operatorType = args[1];

        // All args after operator type are operator arguments
        List<String> operatorArgs = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            operatorArgs.add(args[i]);
        }

        // Load downstream info—this would come from a config or leader message
        List<DownstreamTarget> downstream = RoutingLoader.load(taskId);

        WorkerTask worker = new WorkerTask(taskId, operatorType, operatorArgs, downstream);
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
                forwardTuple(line);
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
            } catch (Exception ignored) {}
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

    private void handleClient(Socket client) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                opStdin.write(line + "\n");
                opStdin.flush();
            }
        } catch (Exception ignored) {}
    }
}

// Simple class for downstream info
// TODO likely will need to be on its own file and be used determine routing
class DownstreamTarget {
    public String host;
    public int port;
    public int taskId;
}

// Stub for fetching routing info
// TODO: get list of subsequent tasks from leader for this taskId
// eg. task 2 -> tasks 5,6,7
class RoutingLoader {
    public static List<DownstreamTarget> load(int taskId) {
        // leader will place routing files on VM
        return new ArrayList<>();
    }
}