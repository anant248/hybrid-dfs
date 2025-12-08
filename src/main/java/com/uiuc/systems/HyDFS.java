package com.uiuc.systems;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HyDFS implements the distributed file system logic for handling file operations 
 * (create, append, get, merge, rereplicate, rebalance, etc.) across multiple nodes.
 * It interacts with the consistent hashing ring to determine file placement and 
 * replication, and uses sockets to coordinate operations between nodes.
 */

public class HyDFS {
//    private static final Logger logger = LoggerFactory.getLogger(HyDFS.class);
    private static final int PORT = 7070; 
    private volatile boolean initialized = false;
    private NodeId currentNode;
    private ConcurrentHashMap<String, FileMetaData> fileMetaMap;
    private ConcurrentHashMap<String, ReentrantLock> fileLocks;
    private Ring ring;

    public HyDFS(NodeId currentNode,Ring ring) {
        fileLocks = new ConcurrentHashMap<>();
        fileMetaMap = new ConcurrentHashMap<>();
        this.currentNode = currentNode;
        this.ring = ring;
    }

    public NodeId getCurrentNode() {
        return currentNode;
    }

    public void setCurrentNode(NodeId currentNode) {
        this.currentNode = currentNode;
    }

    public Ring getRing() {
        return ring;
    }

    public void setRing(Ring ring) {
        this.ring = ring;
    }

    public ConcurrentHashMap<String, FileMetaData> getFileMetaMap() {
        return this.fileMetaMap;
    }

    /*
     * Method used solely for rebalance() logic
     * Sets volatile boolean initialized as true once newly joined node has stabilized (gotten a full membership list/ring)
     */
    public void markInitialized() {
        this.initialized = true;
    }

    /*
     * Method to handle create logic on the recieving node
     * req: the incoming req of type CreateRequest
     * client: the node that sent this req, which is the node we have to ACK/NACK
     * out: an ObjectOutputStream
     * responds with an ACK once quorum replicas have successfully created the file in their hyDFS
     */
    public void createHandler(CreateRequest req, Socket client, ObjectOutputStream out){
        String fileName = req.getFileName();

        // don't need to create, since it's already present - respond NACK
        if(fileMetaMap.containsKey(fileName)){
            sendResponseToClient(client,"NACK",out);
            return;
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        try{

            // get the list of replicas and owner to write to
           List<NodeId> fileReplicas = ring.getReplicas(fileName);
           NodeId owner = fileReplicas.get(0);
           File file = new File("hdfs/" + fileName);
           try (FileOutputStream fos = new FileOutputStream(file)) {
               fos.write(req.getFileData());
           } catch (IOException e) {
//               logger.error("Unable to write to file: /hdfs/" + fileName + "due to error: " + e.getMessage());
               sendResponseToClient(client,"NACK",out);
               return;
           }

           // add to FileMetaMap since we now have it in our hyDFS
           FileMetaData fileMetaData = new FileMetaData(owner,fileReplicas,fileName,0);
           fileMetaMap.put(fileName, fileMetaData);

           int ackCount=1; // count writing to ourselves as an ACK
           List<Future<Boolean>> responses = new ArrayList<>();

           // send to all other replicas
            for(int i=0;i<fileReplicas.size();i++){
               NodeId currentReplica = fileReplicas.get(i);
               if(currentReplica.getIp().equals(owner.getIp())) continue;
               responses.add(executorService.submit(() ->
                       sendCreateRequestToReplica(currentReplica,req.getFileData(),fileName)
               ));
           }

           // count how many ACK's I recieved from the other nodes I wrote to
            for (Future<Boolean> f : responses) {
                try {
                    if (f.get()) ackCount++;
                } catch (Exception e) {
//                    logger.error(e.getMessage());
                }
            }

            // respond with an ACK to the client once quorum number of nodes have also ACK'd me back
           int writeQuorum = (fileReplicas.size()/2)+1;
           if(ackCount >= writeQuorum){
               sendResponseToClient(client, "ACK",out);
           }
           else{
               sendResponseToClient(client,"NACK",out);
           }
        }
        catch (Exception e){
//            logger.error(e.getMessage());
            sendResponseToClient(client, "NACK",out);
        } finally {
            executorService.shutdown();
        }
    }

    /*
     * Method to handle create logic on the replica recieving node
     * req: the incoming req of type CreateRequest
     * client: the node that sent this req, which is the node we have to ACK/NACK
     * out: an ObjectOutputStream
     * responds with an ACK once we have successfully created the file in our hyDFS
     */
    public void handleReplicaCreateRequest(CreateRequest replicaCreateRequest, Socket client,ObjectOutputStream out){
        String fileName = replicaCreateRequest.getFileName();
        File file = new File("hdfs/"+fileName);
        List<NodeId> fileReplicas = ring.getReplicas(fileName);
        NodeId owner = fileReplicas.get(0);

        // file already exists, don't need to create it again - respond NACK
        if(file.exists() && fileMetaMap.containsKey(fileName)){
            sendResponseToClient(client,"NACK",out);
            return;
        }

        // try writing the file
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(replicaCreateRequest.getFileData());
        } catch (Exception e) {
            sendResponseToClient(client,"NACK",out);
            return;
        }

        // add this new file to our FileMetaMap with correct details
        fileMetaMap.put(fileName, new FileMetaData(owner,fileReplicas,fileName,0));
        sendResponseToClient(client, "ACK",out);
    }

    /**
     * Sends a create request from an owner node to a replica node
     * replica: Node to send the create request to
     * filedata: data to send
     * filename: name of hyDFS file to create
     * return true if replica acknowledged the creation, false otherwise.
     */
    public boolean sendCreateRequestToReplica(NodeId replica,byte[] fileData,String fileName){
        String replicaIp = replica.getIp();
        try (Socket replicaSocket = new Socket(replicaIp, PORT);
            ObjectInputStream in = new ObjectInputStream(replicaSocket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(replicaSocket.getOutputStream());)
        {
            CreateRequest replicaCreateRequest = new CreateRequest(fileData, fileName);
            out.writeObject(replicaCreateRequest);
            out.flush();
            String response = (String) in.readObject();
            return response.equals("ACK");
        } catch (Exception e){
//            logger.error(e.getMessage());
            System.out.println("Unsuccessful create to replica");
            return false;
        }
    }

    /**
     * Sends a create request from the client to the file’s owner node to create a file in HyDFS.
     * Reads file data from local `inputs/` directory.
     * return true if owner successfully created the file and achieved quorum, false otherwise.
     */
    public boolean sendCreateRequestToOwner(String localFilePath, String hdfsFileName){
        List<NodeId> hdfsFileReplicas = ring.getReplicas(hdfsFileName);

        // if no replicas, return false
        if(hdfsFileReplicas.size() == 0){
            return false;
        }
        NodeId owner = hdfsFileReplicas.get(0);
        String ownerIp = owner.getIp();

        try(Socket socket = new Socket(ownerIp, PORT);)
        {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            byte[] fileData = Files.readAllBytes(Paths.get("inputs",localFilePath));
            CreateRequest ownerCreateRequest = new CreateRequest(fileData,hdfsFileName);

            out.writeObject(ownerCreateRequest);
            out.flush();

            String response = (String) in.readObject();
            return response.equals("ACK");
        }
        catch (NoSuchFileException | EOFException e){
            System.out.println("Local inputs/ directory contains no file named: " + localFilePath);
            return false;
        }
        catch(Exception e){
//            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * Creates an empty file in HyDFS by sending a CREATE request to the owner,
     * but without uploading any initial content.
     *
     * @param hdfsFileName the HyDFS filename to create
     * @return true if file creation succeeded, false otherwise
     */
    public boolean sendCreateEmptyFileToOwner(String hdfsFileName) {
        try {
            // Determine the owner node for this filename
            List<NodeId> hdfsFileReplicas = ring.getReplicas(hdfsFileName);

            // if no replicas, return false
            if(hdfsFileReplicas.size() == 0){
                return false;
            }
            NodeId owner = hdfsFileReplicas.get(0);
            String ownerIp = owner.getIp();

            // Open socket to owner
            try (Socket socket = new Socket(ownerIp, PORT);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                out.flush();  // important
                CreateRequest ownerCreateRequest = new CreateRequest(new byte[0], hdfsFileName);

                out.writeObject(ownerCreateRequest);
                out.flush();

                // Wait for response
                String response = (String) in.readObject();
                return response.equals("ACK");
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void sendResponseToClient(Socket client, String message,ObjectOutputStream out){
        try{
            out.writeObject(message);
            out.flush();
        }
        catch (IOException e){
//            logger.error("Unable to send response back to client: ", e.getMessage());
            System.out.println("Unable to response back to client");
        }
    }

    /**
     * Sends an append request from client to owner to append data from local file to an existing HyDFS file.
     * return true if append succeeded and quorum was achieved.
     */
    public boolean sendAppendRequestToOwner(String localFilePath, String hdfsFileName){
        List<NodeId> hdfsFileReplicas = verifyExistingReplicas(ring.getReplicas(hdfsFileName),hdfsFileName);
        
        // if no replicas, return false
        if(hdfsFileReplicas.size() == 0){
            return false;
        }
        NodeId owner = hdfsFileReplicas.get(0);
        String ownerIp = owner.getIp();

        try(Socket socket = new Socket(ownerIp, PORT);)
        {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            byte[] fileData = Files.readAllBytes(Paths.get("inputs",localFilePath));
            AppendRequest ownerAppendRequest = new AppendRequest(fileData,hdfsFileName);

//            logger.info("Starting append...");
            System.out.println("Starting append...");

            out.writeObject(ownerAppendRequest);
            out.flush();

            String response = (String) in.readObject();
//            logger.info("Append completed");
            System.out.println("Append completed");
            return response.equals("ACK");
        }
        catch(Exception e){
//            logger.error("Append incomplete with issue: " + e.getMessage());
            System.out.println("Append incomplete with issue: " + e.getMessage());
            return false;
        }
    }

    /* Sends an append request from client to owner to append a raw tuple (already in-memory),
     * instead of reading from a local file. Used by MP4 worker final stage.
     */
    private boolean sendAppendTupleRequestToOwner(String hdfsFileName, String tuple) {
        // List<NodeId> hdfsFileReplicas = verifyExistingReplicas(ring.getReplicas(hdfsFileName), hdfsFileName);

        // if (hdfsFileReplicas.size() == 0) {
        //     return false;
        // }

        // NodeId owner = hdfsFileReplicas.get(0);
        // String ownerIp = owner.getIp();
        String ownerIp = "fa25-cs425-7601.cs.illinois.edu";

        try (Socket socket = new Socket(ownerIp, PORT)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Convert tuple to bytes
            byte[] tupleData = (tuple + "\n").getBytes(StandardCharsets.UTF_8);

            OutputTupleAppendRequest ownerAppendRequest = new OutputTupleAppendRequest(tupleData, hdfsFileName);

            out.writeObject(ownerAppendRequest);
            out.flush();

            String response = (String) in.readObject();
            return response.equals("ACK");
        } catch (Exception e) {
//            logger.error("AppendTuple incomplete with issue: " + e.getMessage());
            return false;
        }
    }

    /**

    /**
     * Used by MP4 Stream Processor final stage task
     * Append the given tuple to the specified HyDFS file
     */
    public boolean appendTuple(String hdfsFileName, String tuple) {
        try {
            
            // Forward request to file owner
            return sendAppendTupleRequestToOwner(hdfsFileName, tuple);

        } catch (Exception e) {
            System.err.println("HyDFS appendTuple error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Sends multiple concurrent append requests to multiple nodes 
     * for distributed concurrent appending to the same HyDFS file.
     */
    public void sendMultiAppendRequestToNodes(String[] clients, String[] localFiles, String hdfsFileName){
        ExecutorService executor = Executors.newCachedThreadPool();

        // open a socket and append to all clients specified in cmd input
        for(int i=0;i<clients.length;i++){
            String clientIp = clients[i];
            String localFileName = localFiles[i];
            executor.submit(() -> {
                try(Socket sc = new Socket(clientIp,PORT);) {
                    ObjectOutputStream out = new ObjectOutputStream(sc.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(sc.getInputStream());
                    MultiAppendRequest multiAppendRequest = new MultiAppendRequest(localFileName,hdfsFileName);
                    out.writeObject(multiAppendRequest);
                    out.flush();
                    String response = (String) in.readObject();
                    if(response.equals("ACK")){
                        System.out.println("Append successful from client: "+clientIp);
                    }
                    else{
                        System.out.println("Append failed from client: "+clientIp);
                    }
                } catch (Exception e) {
                    System.out.println("Append failed from client: "+clientIp);
//                    logger.error("Append failed with issue: " + e.getMessage());
                }
            });
        }
        executor.shutdown();
    }

    /**
     * Initiates a merge request from client to the file’s owner.
     * Ensures that all replicas have identical file content after concurrent appends.
     */
    public void sendMergeRequestToOwner(String hdfsFileName){
        List<NodeId> hdfsFileReplicas = verifyExistingReplicas(ring.getReplicas(hdfsFileName),hdfsFileName);

        // if no replicas, nothing to merge
        if(hdfsFileReplicas.size() == 0){
            System.out.println("No replicas for file: "+ hdfsFileName);
            return;
        }
        NodeId owner = hdfsFileReplicas.get(0);
        String ownerIp = owner.getIp();

        // initiate merge request to owner (primary replica node)
        try(Socket socket = new Socket(ownerIp,PORT)) {
//            logger.info("Merge started...");
            System.out.println("Merge started...");
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            MergeRequest mergeRequest = new MergeRequest(hdfsFileName,hdfsFileReplicas);
            out.writeObject(mergeRequest);
            out.flush();
//            logger.info("Merge complete");
            System.out.println("Merge complete");
        } catch (Exception e){
//            logger.error("Merge incomplete with issue: " + e.getMessage());
            System.out.println("Merge incomplete with issue: " + e.getMessage());
        }
    }

    /**
     * Handles the merge operation at the file’s owner node.
     * Compares lastAppendIndices across replicas, identifies stale ones,
     * and synchronizes them by re-sending the full file from owner to outdated replicas.
     * Logs timing metrics for performance evaluation.
     */
    public void mergeHandler(MergeRequest req, Socket client, ObjectOutputStream out){
        String fileName = req.getFileName();
        List<NodeId> fileReplicas = req.getReplicas();
        NodeId owner = fileReplicas.get(0);
        File file = new File("hdfs/"+fileName);
        FileMetaData fileMeta = fileMetaMap.get(fileName);

        // file to merge should be present im hdfs/
        if(!file.exists() || fileMeta == null){
            System.out.println("The following does not exist in the primary replica: "+ fileName);
            return;
        }

        long mergeStart = System.currentTimeMillis();
//        logger.info("[MergeStart] file=" + fileName + " ts=" + mergeStart);

        int ownerLastIndex = fileMeta.getLastIndex();
        List<NodeId> replicasThatNeedMerge = new ArrayList<NodeId>();

        // determine which replicas actually need a merge (where the last append index is inconsistent)
        for(NodeId replica: fileReplicas){
            String replicaIp = replica.getIp();

            // don't need to merge on myself
            if(replicaIp.equals(owner.getIp())) continue;

            try(Socket sc = new Socket(replicaIp,PORT);
                ObjectOutputStream out1 = new ObjectOutputStream(sc.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(sc.getInputStream());){
                out1.writeObject(new LastIndexCheckRequest(fileName));
                out1.flush();
                Integer response = (Integer) in.readObject();
                if(response != ownerLastIndex){
                    replicasThatNeedMerge.add(replica);
                }
             } catch(Exception e){
                // handle exception
                System.out.println("An error occurred while fetching lastAppendIndex from the replica node: "+ replicaIp);
            }
        }

        // merge on the replicas that need merging
        if(replicasThatNeedMerge.size()>0){
            ExecutorService executor = Executors.newCachedThreadPool();
            try {
                byte[] fileBytes = Files.readAllBytes(Paths.get("hdfs", fileName));
                // send the whole file from owner hdfs to replica nodes simultaneously
                for (NodeId replica : replicasThatNeedMerge) {
                    String replicaIp = replica.getIp();
                    executor.submit(() -> {
                        try (Socket sc1 = new Socket(replicaIp, PORT);
                             ObjectOutputStream out2 = new ObjectOutputStream(sc1.getOutputStream());
                             ObjectInputStream in1 = new ObjectInputStream(sc1.getInputStream());) {
                            MergeReplicaRequest mergeReplicaRequest = new MergeReplicaRequest(fileBytes, fileName,ownerLastIndex);
                            out2.writeObject(mergeReplicaRequest);
                            out2.flush();
                            String res = (String) in1.readObject();
                            if (res.equals("NACK")) {
                                System.out.println("Merge failed for file" + fileName + "on replica: " + replicaIp);
                            } else {
                                System.out.println("Merge successful for file" + fileName + "on replica: " + replicaIp);
                            }
                        } catch (Exception e) {
                            System.out.println("Merge failed for file" + fileName + "on replica: " + replicaIp);
                        }
                    });
                }
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                executor.shutdown();
            }
        }
        else{
            System.out.println("All replicas are up to date");
        }

        long mergeEnd = System.currentTimeMillis();
//        logger.info("[MergeEnd] file=" + fileName + " ts=" + mergeEnd);
//        logger.info("Merge time = " + (mergeEnd - mergeStart) + " ms");
    }

    /**
     * Handles a request from the owner node for checking the replica’s last append index.
     */
    public void handleReplicaLastIndexCheckRequest(LastIndexCheckRequest req, ObjectOutputStream out){
        String fileName = req.getFileName();
        FileMetaData fileMetaData = fileMetaMap.get(fileName);
        Integer replicaIndex = fileMetaData.getLastIndex();
        try {
            out.writeObject(replicaIndex);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Handles incoming merge requests at replica nodes.
     * Overwrites file content with owner’s version and updates metadata.
     */
    public void handleReplicaMergeRequest(MergeReplicaRequest req, Socket client, ObjectOutputStream out) {
        String fileName = req.getFileName();
        int ownersLastIndex = req.getIndex();
        File file = new File("hdfs/" + fileName);
        try (FileOutputStream fos = new FileOutputStream(file, false)) {
            fos.write(req.getFileData());
            fos.flush();

            FileMetaData meta = fileMetaMap.get(fileName);
            if (meta != null) {
                meta.setLastIndex(ownersLastIndex);
            }
            sendResponseToClient(client, "ACK",out);
            out.flush();
        } catch (IOException e) {
            sendResponseToClient(client, "NACK",out);
            e.printStackTrace();
        }
    }

    /**
     * Handles append request specifically for tuples meant to go to the output file
     * Will only be used at VM1 since the OutputTupleAppendRequest is only sent to VM1 node
     * @param req
     * @param client
     * @param out
     */
    public void outputTupleAppendHandler(OutputTupleAppendRequest req, Socket client, ObjectOutputStream out) {
        String fileName = req.getFileName();

        try {
            File file = new File("hdfs/"+fileName);

            try (FileOutputStream fos = new FileOutputStream(file, true)) {
                fos.write(req.getData());
                sendResponseToClient(client, "ACK",out);
                
            } catch(IOException e){
                e.printStackTrace();
                sendResponseToClient(client, "NACK",out);
                return;
            }
        } catch(Exception e){
            e.printStackTrace();
            sendResponseToClient(client, "NACK",out);
        }
    }

    /**
     * Handles append requests at the owner node.
     * Appends data locally and propagates the change to replicas while maintaining order via lastAppendIndex.
     */
    public void appendHandler(AppendRequest req, Socket client,ObjectOutputStream out){
        String fileName = req.getFileName();
        ReentrantLock fileLock = fileLocks.computeIfAbsent(fileName, f -> new ReentrantLock());
        fileLock.lock();
        ExecutorService executorService = Executors.newCachedThreadPool();
        try{
            File file = new File("hdfs/"+fileName);
            FileMetaData fileMeta = fileMetaMap.get(fileName);
            if(!file.exists() || fileMeta == null){
                sendResponseToClient(client, "NACK",out);
                return;
            }
            List<NodeId> fileReplicas = verifyExistingReplicas(ring.getReplicas(fileName),fileName);
            NodeId owner = fileReplicas.get(0);
            int nextAppendIndex = fileMeta.nextLastIndex();
            try (FileOutputStream fos = new FileOutputStream(file, true)) {
                fos.write(req.getData());
            } catch(IOException e){
                e.printStackTrace();
                sendResponseToClient(client, "NACK",out);
                return;
            }

            fileMeta.setLastIndex(nextAppendIndex);

            int ackCount=1;
            List<Future<Boolean>> responses = new ArrayList<>();
            for(int i=0;i<fileReplicas.size();i++){
                NodeId currentReplica = fileReplicas.get(i);
                if(currentReplica.getIp().equals(owner.getIp())) continue;
                responses.add(executorService.submit(() ->
                        sendAppendRequestToReplica(currentReplica,req.getData(),fileName,nextAppendIndex)
                ));
            }

            for (Future<Boolean> f : responses) {
                try {
                    if (f.get()) ackCount++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            int writeQuorum = (fileReplicas.size()/2)+1;
            if(ackCount >= writeQuorum){
                sendResponseToClient(client, "ACK",out);
            }
            else{
                sendResponseToClient(client,"NACK",out);
            }
        } catch(Exception e){
            e.printStackTrace();
            sendResponseToClient(client, "NACK",out);
        } finally {
            fileLock.unlock();
            executorService.shutdown();
        }
    }

    /**
     * Sends an append request from owner to replica node to replicate appended data.
     * return true if replica acknowledged the append.
     */
    public boolean sendAppendRequestToReplica(NodeId replica,byte[] fileData, String fileName, int nextAppendIndex){
        String replicaIp = replica.getIp();
        try (Socket replicaSocket = new Socket(replicaIp, PORT);)
        {
            ObjectOutputStream out = new ObjectOutputStream(replicaSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(replicaSocket.getInputStream());
            AppendRequest replicaAppendRequest = new AppendRequest(fileData, fileName, nextAppendIndex);
            out.writeObject(replicaAppendRequest);
            out.flush();
            String response = (String) in.readObject();
            return response.equals("ACK");
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Handles append requests received by replica nodes.
     * Ensures ordering correctness using append indices.
     */
    public void handleReplicaAppendRequest(AppendRequest req,Socket client,ObjectOutputStream out){
        String fileName = req.getFileName();
        ReentrantLock fileLock = fileLocks.computeIfAbsent(fileName, f -> new ReentrantLock());
        fileLock.lock();
        try {
            File file = new File("hdfs/"+fileName);
            FileMetaData fileMeta = fileMetaMap.get(fileName);
            if(!file.exists() || fileMeta == null){
                sendResponseToClient(client,"NACK",out);
                return;
            }

            int lastAppendIndex = fileMeta.getLastIndex();
            int nextAppendIndex = req.getAppendIndex();
            if(lastAppendIndex+1 == nextAppendIndex) {
                try (FileOutputStream fos = new FileOutputStream(file, true)) {
                    fos.write(req.getData());
                }
                fileMeta.setLastIndex(nextAppendIndex);
                sendResponseToClient(client,"ACK",out);
            } else if(nextAppendIndex <= lastAppendIndex){
                sendResponseToClient(client,"ACK",out);
            }
            else{
                sendResponseToClient(client, "NACK",out);
            }
        } catch(IOException e){
            e.printStackTrace();
            sendResponseToClient(client,"NACK",out);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * Retrieves a HyDFS file from its owner node and stores it locally in the `output/` folder.
     * return true if file successfully retrieved and saved.
     */
    public boolean getHyDFSFileToLocalFileFromOwner(String hdfsFileName, String localFileName) {

        // check if the owner actually has the file
        List<NodeId> hdfsFileReplicas = verifyExistingReplicas(ring.getReplicas(hdfsFileName),hdfsFileName);

        // if no replicas, nothing to merge
        if(hdfsFileReplicas.size() == 0){
            System.out.println("No replicas for file: "+ hdfsFileName);
            return false;
        }

        // getting the hyDFS file from primary replica node
        NodeId owner = hdfsFileReplicas.get(0);

        try (Socket socket = new Socket(owner.getIp(), PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // send GetRequest
            GetRequest getReq = new GetRequest(hdfsFileName);
            out.writeObject(getReq);
            out.flush();

            Object response = in.readObject();
            // if response is NACK then get was unsuccessful
            if (response instanceof String && ((String) response).equals("NACK")) {
                return false;
            } 
            // if response was bytes, then write it to localfilename
            else if (response instanceof byte[]) {
                byte[] fileData = (byte[]) response;
                String localDir = "output/";
                Files.createDirectories(Paths.get(localDir)); // ensures folder exists
                try (FileOutputStream fos = new FileOutputStream(localDir + localFileName)) {
                    fos.write(fileData);
                }
                return true;
            } else {
                System.err.println("Unexpected response type from replica");
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Retrieves a HyDFS file from a specified replica node.
     * Useful for verifying replica consistency or redundancy.
     * return true if file successfully retrieved and saved locally.
     */
    public boolean getHyDFSFileToLocalFileFromReplica(String vmAddress, String hdfsFileName, String localFileName) {
        try (Socket socket = new Socket(vmAddress, PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // send GetRequest to the specific replica
            GetRequest getReq = new GetRequest(hdfsFileName);
            out.writeObject(getReq);
            out.flush();

            Object response = in.readObject();

            // if response is NACK then get was unsuccessful
            if (response instanceof String && ((String) response).equals("NACK")) {
                System.err.println("Replica at " + vmAddress + " does not have file: " + hdfsFileName);
                return false;
            } 
            // if response was bytes, then write it to localfilename
            else if (response instanceof byte[]) {
                byte[] fileData = (byte[]) response;
                String localDir = "output/";
                Files.createDirectories(Paths.get(localDir)); // ensures folder exists
                try (FileOutputStream fos = new FileOutputStream(localDir + localFileName)) {
                    fos.write(fileData);
                }
                return true;
            } else {
                System.err.println("Unexpected response type from replica " + vmAddress);
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Handles an incoming get request on a replica or owner node.
     * Responds with file bytes or NACK if file not found.
     */
    public void getHandler(GetRequest req, Socket client,ObjectOutputStream out) {
        String fileName = req.getFileName();
        FileMetaData fileMeta = fileMetaMap.get(fileName);

//        if (fileMeta == null) {
//            sendResponseToClient(client, "NACK",out);
//            return;
//        }

        File file = new File("hdfs/" + fileName);
        if (!file.exists()) {
            sendResponseToClient(client, "NACK",out);
            return;
        }

        try (FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }

            out.writeObject(baos.toByteArray());
            out.flush();

        } catch (IOException e) {
            e.printStackTrace();
            sendResponseToClient(client, "NACK",out);
        }
    }

    /*
     * Method to check whether current node contains filename in its hyDFS
     * @param filename: hyDFS file to check
     * @return boolean: true if its contains, falls otherwise
     */
    public boolean hasFile(String fileName) {
        return fileMetaMap.containsKey(fileName)
            && new File("hdfs/" + fileName).exists();
    }

    /*
     * Method to check which replicas actually contains the file, those are the valid replica nodes
     * @param candidates: list of all replica nodes
     * @param filename: name of hyDFS file to check
     * @return List<NodeId>: list of valid node id's that contain the file in their hyDFS
     */
    public List<NodeId> verifyExistingReplicas(List<NodeId> candidates, String fileName) {
        List<NodeId> valid = new ArrayList<>();
        for (NodeId node : candidates) {
            try (Socket socket = new Socket(node.getIp(), PORT);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                out.writeObject(new FileCheckRequest(fileName));
                out.flush();
                String resp = (String) in.readObject();
                if ("YES".equals(resp)) valid.add(node);
            } catch (Exception e) {
                //handle the exception?
                System.out.println("Node " + node.getIp() + " did not contain the hdfs file " + fileName);
                
            }
        }
        return valid;
    }

    /*
     * Method called anytime the ring loses a node
     * ensures every file’s replicas are consistent with the current ring view
     * Goes through the file meta map for all files, calculates the replicas that SHOULD be for that file, compares if the replicas in the metadata of that file are the same
     * If list of expected replicas and that files actual replicas match, do nothing
     * Otherwise, open a socket to the new replica, write the file to the new node and have it update its own file meta map (if another node didnt beat you to writing),
     * then update your own filemeta map to now use the new set of replicas instead of the old one
     * No argument or return value, simply modifies/updates the file meta map for that node
     */
    public void rereplicate() {
        for (Map.Entry<String, FileMetaData> entry : fileMetaMap.entrySet()) {
            String fileName = entry.getKey();
            FileMetaData meta = entry.getValue();
            
            // expected replica list based on latest ring structure
            List<NodeId> expectedReplicas = ring.getReplicas(fileName);
            int lastIndex = meta.getLastIndex();

            // Case 1: identical replica list → nothing to do
            if (expectedReplicas.equals(meta.getReplicas())) continue;

            // Case 2: ring is now smaller than 5, so no need for re-replication
            if (expectedReplicas.size() < 5) {
                meta.setReplicas(expectedReplicas);
                fileMetaMap.put(fileName, meta);
                continue;
            }

            // Case 3: re-replication needed
            // start re-replication timer
            long reReplicaStart = System.currentTimeMillis();
            long dataTransferSize = 0;
            // System.out.println("[ReReplicationStart] ts = " + reReplicaStart + " ms");
//            logger.info("[ReReplicationStart] ts = " + reReplicaStart + " ms");

            Set<NodeId> oldSet = new HashSet<>(meta.getReplicas());
            Set<NodeId> newSet = new HashSet<>(expectedReplicas);
            newSet.removeAll(oldSet); // only keep the node(s) that needs re-replication in the newSet

            for (NodeId newReplica : newSet) {
                try {
                    byte[] fileData = Files.readAllBytes(Paths.get("hdfs/" + fileName));
                    dataTransferSize += fileData.length;

                    // send as ReReplicaRequest
                    ReReplicaRequest req = new ReReplicaRequest(fileName, fileData, expectedReplicas, lastIndex);

                    sendReReplicaRequestToNode(newReplica, req);
                    System.out.println("Re-replicated file " + fileName + " to " + newReplica);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // end re-replication timer
            long reReplicaEnd = System.currentTimeMillis();
            // System.out.println("[ReReplicationEnd] ts = " + reReplicaEnd + " ms");
//            logger.info("[ReReplicationEnd] ts = " + reReplicaEnd + " ms");
//            logger.info("Total ReReplication Time, tr = " + (reReplicaEnd - reReplicaStart) + " ms");
//            logger.info("Total Data Transfer, dt = " + dataTransferSize + " bytes");

            // Update metadata after successful replication
            meta.setOwner(expectedReplicas.get(0));
            meta.setReplicas(expectedReplicas);
            fileMetaMap.put(fileName, meta);
        }
    }

    /*
     * Helper method for rereplicate
     * Sends the rereplicate request to the new replica node
     */
    public boolean sendReReplicaRequestToNode(NodeId node, ReReplicaRequest req) {
        try (Socket socket = new Socket(node.getIp(), PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(req);
            out.flush();

            String response = (String) in.readObject();
            return "ACK".equals(response);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /*
     * Method to handle the incoming rereplica request
     */
    public void handleReReplicaRequest(ReReplicaRequest req, Socket client, ObjectOutputStream out) {
        String fileName = req.getFileName();
        File file = new File("hdfs/" + fileName);

        if (file.exists() && fileMetaMap.containsKey(fileName)) {
            sendResponseToClient(client, "NACK", out);
            return;
        }

        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(req.getFileData());
        } catch (Exception e) {
            e.printStackTrace();
            sendResponseToClient(client, "NACK", out);
            return;
        }

        // Update this node’s metadata
        NodeId owner = req.getReplicas().get(0);
        FileMetaData meta = new FileMetaData(owner, req.getReplicas(), fileName, req.getLastIndex());
        fileMetaMap.put(fileName, meta);

        sendResponseToClient(client, "ACK", out);
    }

    /*
     * Method called anytime the ring gains a node
     * Goes through the file meta map for all files, calculates the replicas that SHOULD be for that file, compares if the replicas in the metadata of that file are the same
     * No argument or return value, simply modifies/updates the file meta map for that node
     */
    public void rebalance() {
        if (fileMetaMap.isEmpty()) return; // to avoid Skipping rebalance print statements upon initial joins

        if (!initialized) {
            System.out.println("Skipping rebalance; node still initializing.");
            return;
        }

        for (Map.Entry<String, FileMetaData> entry : fileMetaMap.entrySet()) {
            String fileName = entry.getKey();
            FileMetaData meta = entry.getValue();
            List<NodeId> expectedReplicas = ring.getReplicas(fileName);
            List<NodeId> currentReplicas = meta.getReplicas();

            NodeId self = currentNode;

            // CASE 1: identical replica list → nothing to do
            if (expectedReplicas.equals(currentReplicas)) continue;

            // CASE 2: this node should NOT be a replica anymore
            // “If I currently have the file, but I’m no longer supposed to, remove it.”
            if (!expectedReplicas.contains(self) && currentReplicas.contains(self)) {
                // remove file from disk and metadata
                File file = new File("hdfs/" + fileName);
                if (file.exists()) file.delete();
                fileMetaMap.remove(fileName);
                System.out.println("Removed file " + fileName + " from this node due to rebalancing.");
                continue;
            }

            // CASE 3: this node should keep the file but replicas list changed
            // same as re-replication logic
            if (!expectedReplicas.equals(currentReplicas)) {
                // start re-balance timer
                long reBalanceStart = System.currentTimeMillis();
                long dataTransferSize = 0;
                // System.out.println("[ReBalanceStart] ts = " + reBalanceStart + " ms");
//                logger.info("[ReBalanceStart] ts = " + reBalanceStart + " ms");

                Set<NodeId> oldSet = new HashSet<>(currentReplicas);
                Set<NodeId> newSet = new HashSet<>(expectedReplicas);
                newSet.removeAll(oldSet); // // only keep the node(s) that needs re-replication in the newSet

                for (NodeId newReplica : newSet) {
                    try {
                        byte[] fileData = Files.readAllBytes(Paths.get("hdfs/" + fileName));
                        dataTransferSize += fileData.length;

                        ReReplicaRequest req = new ReReplicaRequest(fileName, fileData, expectedReplicas, meta.getLastIndex());
                        sendReReplicaRequestToNode(newReplica, req);
                        System.out.println("Rebalanced file " + fileName + " to " + newReplica.getIp());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // end re-balance timer
                long reBalanceEnd = System.currentTimeMillis();
                // System.out.println("[ReBalanceEnd] ts = " + reBalanceEnd + " ms");
//                logger.info("[ReBalanceEnd] ts = " + reBalanceEnd + " ms");
//                logger.info("Total ReBalance Time, tr = " + (reBalanceEnd - reBalanceStart) + " ms");
//                logger.info("Total Data Transfer, dt = " + dataTransferSize + " bytes");

                // update metadata for this node
                meta.setOwner(expectedReplicas.get(0));
                meta.setReplicas(expectedReplicas);
                fileMetaMap.put(fileName, meta);
            }
        }
    }


    /**
     * Prints out the contents of fileMetaMap in a human-readable format.
     */
    public void printFileMetaMap() {
        System.out.println("----- File Meta Map -----");
        if (fileMetaMap.isEmpty()) {
            System.out.println("No files stored in HyDFS on this node.");
            return;
        }
        for (Map.Entry<String, FileMetaData> entry : fileMetaMap.entrySet()) {
            String fileName = entry.getKey();
            FileMetaData meta = entry.getValue();
            System.out.println("File Name: " + fileName);
            NodeId owner = meta.getOwner();
            if (owner != null) {
                System.out.println("  Owner Node: " + owner.getIp() + ":" + owner.getPort());
            } else {
                System.out.println("  Owner Node: null");
            }
            List<NodeId> replicas = meta.getReplicas();
            System.out.print("  Replica List: ");
            if (replicas != null && !replicas.isEmpty()) {
                List<String> replicaIps = new ArrayList<>();
                for (NodeId node : replicas) {
                    replicaIps.add(node.getIp());
                }
                System.out.println(replicaIps);
            } else {
                System.out.println("[]");
            }
            System.out.println("  Last Index: " + meta.getLastIndex());
            System.out.println("-------------------------");
        }
    }
}


