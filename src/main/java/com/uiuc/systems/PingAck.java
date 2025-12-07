package com.uiuc.systems;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.uiuc.systems.NodeInfo.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PingAck {
    /* This class contains the logic to send pings to all the other nodes in the membership list and handle
       incoming ACKs, incoming pings, membership updates upon receiving an ACK and timeout check to detect failures.
     */
//    private static final Logger logger = LoggerFactory.getLogger(PingAck.class);
    private static final long tSuspect = 5000;
    private static final long tFail = 2*tSuspect;
    private static final long tCleanup = 2*tFail;

    private Map<NodeId, Long> pendingAcks = new ConcurrentHashMap<>();

    private NodeId currentNode;
    private MemberShipList memberShipList;
    private UDPClient client;

    public PingAck(UDPClient client, MemberShipList memberShipList, NodeId curr) {
        this.client = client;
        this.currentNode = curr;
        this.memberShipList = memberShipList;
    }

    public void sendPing(Boolean suspicionFlag) {
        // check if any pendingAcks have timed out
        if (suspicionFlag) {
            timeoutCheckSuspicion();
        }
        else {
            timeoutCheckNoSuspicion();
        }


        Map<NodeId, NodeInfo> currentMembershipList = memberShipList.getMemberShipList();
        HashSet<String> ips = new HashSet<>();
        for (Map.Entry<NodeId, NodeInfo> entry : currentMembershipList.entrySet()) {
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();
            if (currentNode.equals(nodeId)) {
                // System.out.println("Skipping myself before sending ping");
                nodeInfo.setState(State.ALIVE);
                continue; // skip self
            }
            String currentIp = nodeId.getIp();
            ips.add(currentIp);
            // System.out.println("IPS: " + ips);
            // add to pendingACK's map <ip of the node we are pinging, time we are pinging>
            pendingAcks.putIfAbsent(nodeId, System.currentTimeMillis());
        }
        Message msg = new Message(currentNode, currentMembershipList);
        msg.setMessageType("PING");

        ArrayList<String> pingingIP = new ArrayList<>(ips);
        client.handleSend(pingingIP, msg);
    }

    public void sendAck(NodeId nodeToAck) {
        Map<NodeId, NodeInfo> currentMembershipList = memberShipList.getMemberShipList();
        ArrayList<String> ips = new ArrayList<>();
        ips.add(nodeToAck.getIp());

        // only sending ack 1 node (the node that pinged us, we need to acknowledge)
        Message msg = new Message(currentNode, currentMembershipList);
        msg.setMessageType("ACK");
        client.handleSend(ips, msg);
    }

    public void handleIncomingPingAckSuspicion(Message incomingPing){
        Map<NodeId, NodeInfo> incomingMemberShipList = incomingPing.getMemberShip();
        Map<NodeId, NodeInfo> currentMembershipList = memberShipList.getMemberShipList();
        String msgType = incomingPing.getMessageType();

        if (msgType != null && msgType.equalsIgnoreCase("PING")) {
            // got an incoming ping so send out an ack
            NodeId pingNode = incomingPing.getNode();
            NodeInfo pingNodeInfo = incomingMemberShipList.get(pingNode);

            // if membership list doesnt contain this node that is pinging me , then add it
            if (!currentMembershipList.containsKey(pingNode)) {
                currentMembershipList.put(pingNode, pingNodeInfo);
            }

            sendAck(pingNode); // send ack only back to the node that pinged me
        }
        else if (msgType != null && msgType.equalsIgnoreCase("ACK")) {
            // got an incoming ACK so remove from pending ACK's and then update my membership list
            NodeId ackNode = incomingPing.getNode();
            pendingAcks.remove(ackNode);
            memberShipList.updateMembershipListWithSuspicionPingMode(incomingMemberShipList, currentNode, ackNode);
        }
        timeoutCheckSuspicion();
    }

    public void handleIncomingPingAckNoSuspicion(Message incomingPing){
        Map<NodeId, NodeInfo> incomingMemberShipList = incomingPing.getMemberShip();
        Map<NodeId, NodeInfo> currentMembershipList = memberShipList.getMemberShipList();
        String msgType = incomingPing.getMessageType();

        if (msgType != null && msgType.equalsIgnoreCase("PING")) {
            // got an incoming ping so send out an ack
            NodeId pingNode = incomingPing.getNode();
            NodeInfo pingNodeInfo = incomingMemberShipList.get(pingNode);

            // if membership list doesnt contain this node that is pinging me , then add it
            if (!currentMembershipList.containsKey(pingNode)) {
                currentMembershipList.put(pingNode, pingNodeInfo);
            }

            // System.out.println("Got a ping from this node: "+pingNode.getIp());
            sendAck(pingNode); // send ack only back to the node that pinged me
        }
        else if (msgType != null && msgType.equalsIgnoreCase("ACK")) {
            // got an incoming ACK so update my membership list
            NodeId ackNode = incomingPing.getNode();
            // System.out.println("Got a Incoming ACK from this node: "+ackNode.getIp());
            // System.out.println(ackNode);
            pendingAcks.remove(ackNode);
            memberShipList.updateMembershipListNoSuspicionPingMode(incomingMemberShipList, currentNode, ackNode);
        }
        timeoutCheckNoSuspicion();
    }

    public void timeoutCheckNoSuspicion(){
        // long currentTime = System.currentTimeMillis();
        // System.out.println(pendingAcks);
        Map<NodeId, NodeInfo> currentMemberShipList = memberShipList.getMemberShipList();

        for (Map.Entry<NodeId, Long> entry : pendingAcks.entrySet()) {
            NodeId nodeId = entry.getKey();
            Long pingTime = entry.getValue();

            NodeInfo nodeInfo = currentMemberShipList.get(nodeId);

            if (!currentNode.equals(nodeId) && nodeInfo != null && nodeInfo.getState() == NodeInfo.State.ALIVE && (System.currentTimeMillis() - pingTime) >= tFail) {
                nodeInfo.setState(NodeInfo.State.FAILED);
//                logger.info("In Ping Ack: No suspicion mode, The following node is now failed: " + nodeId.getIp());
                System.out.println("In Ping Ack: No suspicion mode, The following node is now failed: " + nodeId.getIp());
            }
            else if (!currentNode.equals(nodeId) && nodeInfo != null && nodeInfo.getState() == NodeInfo.State.FAILED && (System.currentTimeMillis() - pingTime) >= tCleanup){
                currentMemberShipList.remove(nodeId);
                pendingAcks.remove(nodeId);
//                logger.info("In Ping Ack: No suspicion mode, The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                System.out.println("In Ping Ack: No suspicion mode, The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
            }
        }
    }

    public void timeoutCheckSuspicion(){
        // long currentTime = System.currentTimeMillis();

        for (Map.Entry<NodeId, Long> entry : pendingAcks.entrySet()) {
            NodeId nodeId = entry.getKey();
            Long pingTime = entry.getValue();

            Map<NodeId, NodeInfo> currentMemberShipList = memberShipList.getMemberShipList();
            NodeInfo nodeInfo = currentMemberShipList.get(nodeId);

            if (nodeInfo != null && nodeInfo.getState() == NodeInfo.State.ALIVE && (System.currentTimeMillis() - pingTime) >= tSuspect) {
                nodeInfo.setState(NodeInfo.State.SUSPECT);
//                logger.info("In Ping Ack: Suspicion mode, The following node is now suspected: " + nodeId.getIp());
                System.out.println("In Ping Ack: Suspicion mode, The following node is now suspected: " + nodeId.getIp());
            }
            else if (nodeInfo != null && nodeInfo.getState() == NodeInfo.State.SUSPECT && (System.currentTimeMillis() - pingTime) >= tFail) {
                nodeInfo.setState(NodeInfo.State.FAILED);
//                logger.info("In Ping Ack: Suspicion mode, The following node is now failed: " + nodeId.getIp());
                System.out.println("In Ping Ack: Suspicion mode, The following node is now failed: " + nodeId.getIp());
            }
            else if (nodeInfo != null && nodeInfo.getState() == NodeInfo.State.FAILED && (System.currentTimeMillis() - pingTime) >= tCleanup){
                currentMemberShipList.remove(nodeId);
                pendingAcks.remove(nodeId);
//                logger.info("In Ping Ack: Suspicion mode, The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                System.out.println("In Ping Ack: Suspicion mode, The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
            }
        }
    }
}