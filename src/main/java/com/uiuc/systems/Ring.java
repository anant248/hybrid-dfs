package com.uiuc.systems;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

public class Ring implements Serializable{

    private  TreeMap<BigInteger, NodeId> ring;
    private static final int NUM_REPLICAS = 5;
    private static final int QUORUM = 3;

    public Ring() {
        this.ring = new TreeMap<>();
    }

    // hash function using SHA-1 consistent hashing. Returns the hash as a BigInteger so its easy to compare
    public BigInteger hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] bytes = md.digest(key.getBytes());
            return new BigInteger(1, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Error computing SHA-1 hash", e);
        }
    }

    // adding a new node to the virtual ring
    public void addNode(NodeId node) {
        String key = node.getIp() + ":" + node.getPort();
        ring.put(hash(key), node);
    }

    // removing an exisiting node from the virtual ring
    public void removeNode(NodeId node) {
        String key = node.getIp() + ":" + node.getPort();
        ring.remove(hash(key));
    }

    // rebuilding virtual ring based on current membership list
    public void updateRing(MemberShipList membershipList) {
        ring.clear();
        for (NodeId nodeId : membershipList.getMemberShipList().keySet()) {
            addNode(nodeId);
        }
    }

    // getting List of NodeId's that contain a replica of the file 
    public List<NodeId> getReplicas(String fileName) {
        BigInteger fileHash = hash(fileName);
        Set<NodeId> replicas = new HashSet<>();

        // Edge case — if no nodes are on the ring yet, just return an empty list.
        if (ring.isEmpty()) return replicas.stream().collect(Collectors.toList());

        // gives a view of the ring starting from fileHash clockwise to the end of the ring (the largest hash).
        SortedMap<BigInteger, NodeId> tailMap = ring.tailMap(fileHash);
        Iterator<NodeId> it = tailMap.values().iterator();

        // Add nodes clockwise starting from the first successor until you’ve collected NUM_REPLICAS nodes — or run out of nodes at the end of the map.
        while (replicas.size() < NUM_REPLICAS && it.hasNext()) {
            replicas.add(it.next());
        }

        // Handles the wrap-around case
        if (replicas.size() < NUM_REPLICAS) {
            for (NodeId node : ring.values()) {
                if (replicas.size() >= NUM_REPLICAS) break;
                replicas.add(node);
            }
        }

        return replicas.stream().collect(Collectors.toList());
    }

    // get primary replica (leader node that contains the file)
    public NodeId getPrimaryReplica(String filename) {
        return getReplicas(filename).get(0);
    }

    // print the current view of the ring
    public void printRing() {
        System.out.println("=== Ring State ===");
        for (Map.Entry<BigInteger, NodeId> entry : ring.entrySet()) {
            System.out.println(entry.getValue() + " -> Hash: " + entry.getKey());
        }
    }

    // get the hash value from a NodeId
    public BigInteger getNodeHash(NodeId node) {
        String key = node.getIp() + ":" + node.getPort();
        return hash(key);
    }

    public static int getQuorum() {
        return QUORUM;
    }

    public static int getNumReplicas() {
        return NUM_REPLICAS;
    }

    public TreeMap<BigInteger, NodeId> getRing() {
        return ring;
    }

    public void setRing(TreeMap<BigInteger, NodeId> ring) {
        this.ring = ring;
    }
}