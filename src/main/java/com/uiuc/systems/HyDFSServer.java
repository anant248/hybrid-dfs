package com.uiuc.systems;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class HyDFSServer implements Runnable{
    private HyDFS hdfs;
    private static final int port = 7070;
    private volatile boolean running = true;
    private ServerSocket sc;

    public HyDFSServer(HyDFS hdfs) {
        this.hdfs = hdfs;
    }

    public HyDFS getHdfs() {
        return hdfs;
    }

    public void setHdfs(HyDFS hdfs) {
        this.hdfs = hdfs;
    }

    @Override
    public void run() {
        try {
            sc = new ServerSocket(port);
            sc.setReuseAddress(true);
            System.out.println("Server listening on port " + port);

            while (running) {
                Socket socket = sc.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        } catch (IOException e) {
            if (running) e.printStackTrace(); // ignore if shutting down intentionally
        } finally {
            closeServerSocket();
        }
    }

    /* Method that continuously listens and handles incoming objects from clients */
    private void handleClient(Socket socket) {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            out.flush();
            Object obj = in.readObject();
            // CREATE REQUEST
            if(obj instanceof CreateRequest){
                NodeId currentNode = hdfs.getCurrentNode();
                Ring ring = hdfs.getRing();
                String fileName = ((CreateRequest) obj).getFileName();
                NodeId owner = ring.getPrimaryReplica(fileName);
                if(currentNode.getIp().equals(owner.getIp()))
                {
                    hdfs.createHandler((CreateRequest) obj,socket,out);
                }
                else{
                    hdfs.handleReplicaCreateRequest((CreateRequest) obj, socket,out);
                }
            }
            // APPEND REQUEST
            else if(obj instanceof AppendRequest){
                NodeId currentNode = hdfs.getCurrentNode();
                Ring ring = hdfs.getRing();
                String fileName = ((AppendRequest) obj).getFileName();
                NodeId owner = ring.getPrimaryReplica(fileName);
                if(currentNode.getIp().equals(owner.getIp())){
                    hdfs.appendHandler((AppendRequest) obj,socket,out);
                }
                else{
                    hdfs.handleReplicaAppendRequest((AppendRequest) obj,socket,out);
                }
            // MULTIAPPEND REQUEST
            } else if(obj instanceof MultiAppendRequest){
                MultiAppendRequest req = (MultiAppendRequest) obj;
                String localFile = req.getLocalFileName();
                String hdfsFile = req.getHdfsFileName();
                boolean successfulAppendResponse = hdfs.sendAppendRequestToOwner(localFile, hdfsFile);
                if(successfulAppendResponse){
                   hdfs.sendResponseToClient(socket,"ACK",out);
                }
                else{
                    hdfs.sendResponseToClient(socket,"NACK",out);
                }
            }
            // GET REQUEST
            else if (obj instanceof GetRequest) {
                // handle request to get HyDFS file
                hdfs.getHandler((GetRequest) obj, socket,out);
            }
            // FILE CHECK REQUEST
            else if (obj instanceof FileCheckRequest) {
                // handle request to check if replica node actually has a file
                boolean exists = hdfs.hasFile(((FileCheckRequest) obj).getFileName());
                hdfs.sendResponseToClient(socket, exists ? "YES" : "NO", out);
            } else if (obj instanceof  MergeRequest){
                NodeId currentNode = hdfs.getCurrentNode();
                String fileName = ((MergeRequest) obj).getFileName();
                List<NodeId> replicas = ((MergeRequest) obj).getReplicas();
                NodeId owner = replicas.get(0);
                if(currentNode.getIp().equals(owner.getIp())){
                    hdfs.mergeHandler((MergeRequest) obj,socket,out);
                }
            // LAST INDEX CHECK REQUEST
            } else if (obj instanceof LastIndexCheckRequest){
                hdfs.handleReplicaLastIndexCheckRequest((LastIndexCheckRequest) obj,out);
            // MERGE REPLICA REQUEST
            } else if (obj instanceof MergeReplicaRequest){
                hdfs.handleReplicaMergeRequest((MergeReplicaRequest) obj,socket,out);
            }
            // REREPLICA REQUEST
            else if (obj instanceof ReReplicaRequest) {
                // handle request to rereplicate file onto current node
                hdfs.handleReReplicaRequest((ReReplicaRequest) obj, socket, out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* Method called to stop the HyDFS server , upon Ctrl+C */
    public void stopServer() {
        running = false;
        closeServerSocket();
        System.out.println("HyDFSServer stopped.");
    }

    /* Method called to close server socket , upon Ctrl+C */
    private void closeServerSocket() {
        try {
            if (sc != null && !sc.isClosed()) sc.close();
        } catch (IOException ignored) {}
    }
}
