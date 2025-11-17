package server;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryServer {
    private int port;
    private String logFile;

    // constructor
    public QueryServer(int port, String logFile) {
        this.port = port;
        this.logFile = logFile;
    }

    // starts the server by listening on {port}, blocks while waiting for a client to connect to that port
    // once a client connects, invokes a new Thread to handle that client
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("QueryServer listening on port " + port + " for file " + logFile);

            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new ClientHandler(socket, logFile)).start();
            }
        }
    }

    // Handle behaviour of server once a client connects to that server:port
    private static class ClientHandler implements Runnable {
        private Socket socket;
        private String logFile;

        public ClientHandler(Socket socket, String logFile) {
            this.socket = socket;
            this.logFile = logFile;
        }

        @Override
        // Action taken by the server once client sends a request
        // input (String grepArgs, String query) ex. "-a -b -c" "Hello!"
        // output (int lineCount, String filename) ex. 200 machine.2.log

        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                // read the input query sent by the client
                String grepOptions = in.readLine();
                String query = in.readLine();
                System.out.println("Received request: grep " + grepOptions + " \"" + query + "\" on " + logFile);

                // Build grep command
                List<String> command = new ArrayList<>();
                command.add("grep");

                if (grepOptions != null && !grepOptions.isEmpty()) {
                    String[] opts = grepOptions.trim().split("\\s+");
                    Collections.addAll(command, opts);
                }

                command.add(query);
                command.add(logFile);

                // Run grep using ProcessBuilder
                ProcessBuilder pb = new ProcessBuilder(command);
                Process process = pb.start();

                BufferedReader grepOut = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                String fileName = new java.io.File(logFile).getName(); // e.g., "vm1.log"
                while ((line = grepOut.readLine()) != null) {
                    out.println(line + " : " + fileName);
                }
                out.println("END"); // marker for client to stop reading

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // main() method that is called on each server machine
    // Usage: java src/main/java/server/QueryServer.java <filename>
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java src/main/java/server/QueryServer.java <filename>");
            System.exit(1);
        }
        int port = 6969;
        String fileName = args[0];

        // Build log file path (assuming consistent naming convention)
        String logFile = "" + fileName;

        new QueryServer(port, logFile).start();
    }
}