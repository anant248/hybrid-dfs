package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
    This is a client class which contains the logic to simultaneously open socket connections
    to all the servers on a fixed port, which will also include the current server.
    It makes use of Executors class in java.util.concurrent package to manage the thread's creation
    in a thread pool, allocation and release of thread's resources. Executors.newCachedThreadPool is
    used to create the new worker threads in a pool as required or as new tasks are submitted to the
    ExecutorService. This client assumes that the IP address of each server is final and constant and
    that the server's port is fixed.

    The grep options and the pattern are read as arguments from the main method and are passed simultaneously
    to all the servers through a socket created for that server,executed as a task. As soon as the server responds,
    the task will calculate the number of matching lines and each task will write the count corresponding
    to that server to a concurrent map which will then be used to calculate the sum of matching lines
    across all servers after all tasks have finished execution.
*/
public class DistributedLogQuerierClient {
    //The address and port of each server is final and cannot be changed
    private static final String[] servers = { "fa25-cs425-7601.cs.illinois.edu",  "fa25-cs425-7602.cs.illinois.edu", "fa25-cs425-7603.cs.illinois.edu", "fa25-cs425-7604.cs.illinois.edu", "fa25-cs425-7605.cs.illinois.edu", "fa25-cs425-7606.cs.illinois.edu", "fa25-cs425-7607.cs.illinois.edu", "fa25-cs425-7608.cs.illinois.edu", "fa25-cs425-7609.cs.illinois.edu", "fa25-cs425-7610.cs.illinois.edu" };
    private static final int port = 6969;

    /* This method will open socket connections simultaneously to all the servers and calculate the
    number of matching lines per server and return the sum of all the counts. If a server is down
    then that server will be skipped and the sum of matching line counts from the other non-faulty
    servers is returned. If -c option is passed, then extract the count directly from the response.
    input: Grep options and pattern
    output: Total number of matching lines across all servers
    */
    public static int start(String grepOptions, String query) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        int totalMatchingLinesCount = 0;
        long latencyStart = System.currentTimeMillis();

        for(String server : servers) {
            executorService.execute(() -> {
                try(Socket socket = new Socket(server, port);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);) {
                    writer.println(grepOptions);
                    writer.println(query);
                    String response;
                    int matchingLinesCount = 0;

                    while ((response = reader.readLine()) != null) {
                        if (response.equals("END")) break;
                        if(grepOptions != null && grepOptions.contains("-c")){
                            String[] splitResponse = response.split(":");
                            matchingLinesCount = Integer.parseInt(splitResponse[0].trim());
                        }
                        else {
                            matchingLinesCount++;
                        }
                        synchronized(System.out) {
                            System.out.println(response);
                        }
                    }
                    map.put(server, matchingLinesCount);
                } catch (IOException e) {
                    synchronized (System.err) {
                        System.err.printf("There is a problem in connecting to the server %s on port %d, skipping \n", server, port);
                   }
                }
            });
        }

        executorService.shutdown();
        /* wait for all the tasks to be executed and for the executor service to shut down and then calculate the
           total number of matching lines across all servers */
        try {
            boolean isShutDown = executorService.awaitTermination(1, TimeUnit.MINUTES);
            if(isShutDown) {
                long latencyEnd = System.currentTimeMillis();
                long queryLatency = latencyEnd - latencyStart;
                System.out.println("\nThe query latency was " + queryLatency + " ms\n");

                for(Integer individualCount : map.values()) {
                    totalMatchingLinesCount += individualCount;
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            System.err.println("There was a problem in calculating the sum of total number of matching lines");
        }
        return totalMatchingLinesCount;
    }
    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("Please provide grep options as the first argument and the pattern/text as the second argument");
            System.exit(1);
        }
        String grepOptions = args[0];
        String query = args[1];
        int sum = start(grepOptions, query);
        System.out.println("\n Sum of total number of matching lines: " + sum + "\n");
    }
}
