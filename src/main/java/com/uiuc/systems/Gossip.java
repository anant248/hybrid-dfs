package com.uiuc.systems;

import java.util.ArrayList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gossip {
    /*
       This class is responsible to handle incoming messages received on the UDPServer and send the membership lists to
       all the nodes in the current membership list apart from the self node over UDP using UDPClient. We use Message class
       to build a message which will include the latest membership list and the sender NodeId.
       Upon receiving a message, a handler method will be called based on what protocol mode and suspicion flag is being
       used. The handler methods will merge the local membership list with the incoming membership list if it satisfies the
       requirements(depends on the suspicion flag) and after that a timeout check will be run to detect failures and those
       failed entries from the membership list.
    */
    private static final Logger logger = LoggerFactory.getLogger(Gossip.class);
    private UDPClient client;
    private MemberShipList memberShipList;
    private Ring ring;
    private HyDFS hyDFS;


    private static final long tSuspect = 3000;
    private static final long tFail = 2*tSuspect;
    private static final long tCleanup = 2*tFail;

    private NodeId currentNode;

    public Gossip(UDPClient client, MemberShipList memberShipList, NodeId curr, Ring ring, HyDFS hyDFS) {
        this.client = client;
        this.memberShipList = memberShipList;
        this.currentNode = curr;
        this.ring = ring;
        this.hyDFS = hyDFS;
    }

    /* This method calls the UDPClient handleSend() method to send the message to all the other nodes
       in the membership list.
    */
    public void sendGossip(Boolean suspicionFlag){
        // check if any gossips have timed out
        if (suspicionFlag) {
            timeoutCheckSuspicion();
        }
        else {
            timeoutCheckNoSuspicion();
        }

        Map<NodeId, NodeInfo> currentMembershipList = memberShipList.getMemberShipList();
        ArrayList<String> ips = new ArrayList<>();

        for(Map.Entry<NodeId, NodeInfo> entry: currentMembershipList.entrySet()){
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();

            if(currentNode.equals(nodeId)){
                //CHECK THIS while debugging
                //Do we have to update the lastSeen with the current time of the current node?
                nodeInfo.setLastUpdated(System.currentTimeMillis());
                //Do you also have to change the state to ALIVE for the current node in case of no suspicion- NO?
                nodeInfo.setHeartbeatCounter(nodeInfo.getHeartbeatCounter()+1);
                continue;
            }
            String currentIp = nodeId.getIp();
            ips.add(currentIp);
        }
        
        Message msg = new Message(currentNode, currentMembershipList);
        client.handleSend(ips, msg);
    }

    /* This method is called by UDPServer when it receives a message in Gossip, No suspicion mode.
     * This will merge the incoming membership lists with the local and runs the timeout check to detect failures. (MP3 USED)
     */
    public void handleIncomingGossips(Message incomingGossip){
        Map<NodeId, NodeInfo> incomingMemberShipList = incomingGossip.getMemberShip();
        memberShipList.updateMembershipListNoSuspicion(incomingMemberShipList, currentNode);
        timeoutCheckNoSuspicion();
    }
    /* This method is called by UDPServer when it receives a message in Gossip, No suspicion mode.
       This will merge the incoming membership lists with the local and runs the timeout check to detect failures.
    */
    public void handleIncomingGossipsSuspicion(Message incomingGossip){
        Map<NodeId, NodeInfo> incomingMemberShipList = incomingGossip.getMemberShip();
        memberShipList.updateMembershipListWithSuspicion(incomingMemberShipList, currentNode);
        timeoutCheckSuspicion();
    }

    /* This method checks the timeouts and detects failures in Gossip-No Suspicion mode.
       If a node hasn't been updated or has not received a message within Tfail milliseconds,
       then it will be marked as FAILED and will be removed from the membership list after Tcleanup milliseconds.
    */
    public void timeoutCheckNoSuspicion(){
        Map<NodeId, NodeInfo> currentMemberShipList = memberShipList.getMemberShipList();

        for(Map.Entry<NodeId, NodeInfo> entry: currentMemberShipList.entrySet()){
            NodeId nodeId = entry.getKey();
            NodeInfo nodeInfo = entry.getValue();

            if (!currentNode.equals(nodeId) && nodeInfo.getState() == NodeInfo.State.ALIVE && ((System.currentTimeMillis()) - nodeInfo.getLastUpdated()) >= tFail) {
                nodeInfo.setState(NodeInfo.State.FAILED);
                System.out.println("In Gossip: No Suspicion mode -- The following node is now failed: " + nodeId.getIp());
                logger.info("In Gossip: No Suspicion mode -- The following node is now failed: " + nodeId.getIp());
            } 
            else if (!currentNode.equals(nodeId) && nodeInfo.getState() == NodeInfo.State.FAILED && ((System.currentTimeMillis()) - nodeInfo.getLastUpdated()) >= tCleanup) {
                currentMemberShipList.remove(nodeId);
                ring.removeNode(nodeId);
                System.out.println("In Gossip: No Suspicion mode -- The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                logger.info("In Gossip: No Suspicion mode -- The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                hyDFS.rereplicate();
            }
        }
    }

    /* This method checks the timeouts and detects failures in Gossip-Suspicion mode.
       If a node hasn't been updated or has not received a message within Tsuspect milliseconds,
       then it will be marked as SUSPECT and will be marked as failed after TFail milliseconds
       and will be removed from the membership list after Tcleanup seconds.
    */

    public void timeoutCheckSuspicion(){
        Map<NodeId, NodeInfo> currentMemberShipList = memberShipList.getMemberShipList();

        for(Map.Entry<NodeId, NodeInfo> entry: currentMemberShipList.entrySet()){
            NodeId nodeId = entry.getKey();
            NodeInfo nodeInfo = entry.getValue();
            NodeInfo.State s = nodeInfo.getState();
            long lastSeen = nodeInfo.getLastUpdated();

            if(s == NodeInfo.State.ALIVE){
                if((System.currentTimeMillis()) - lastSeen >= tSuspect){
                    nodeInfo.setState(NodeInfo.State.SUSPECT);
                    nodeInfo.setSuspectTime(System.currentTimeMillis());
                    System.out.println("In Gossip: Suspicion mode -- The following node is now suspected: " + nodeId.getIp());
                    logger.info("In Gossip: Suspicion mode -- The following node is now suspected: "+ nodeId.getIp());
                }
            }
            else if(s == NodeInfo.State.SUSPECT){
                if((System.currentTimeMillis()) - nodeInfo.getSuspectTime() >= tFail){
                    nodeInfo.setState(NodeInfo.State.FAILED);
                    nodeInfo.setFailTime(System.currentTimeMillis());
                    System.out.println("In Gossip: Suspicion mode -- The following node is now failed: " + nodeId.getIp());
                    logger.info("In Gossip: Suspicion mode -- The following node is now failed: "+ nodeId.getIp());
                }
            }
            else if(s == NodeInfo.State.FAILED){
                if((System.currentTimeMillis()) - nodeInfo.getFailTime() >= tCleanup){
                    currentMemberShipList.remove(nodeId);
                    System.out.println("In Gossip: Suspicion mode -- The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                    logger.info("In Gossip: Suspicion mode -- The following node is now cleaned up (removed) from membership: " + nodeId.getIp());
                }
            }
            else continue;
        }
    }
}
