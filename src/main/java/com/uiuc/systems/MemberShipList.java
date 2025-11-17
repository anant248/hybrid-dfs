package com.uiuc.systems;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemberShipList {
    /* This class constructs the membership list for the current node and contains the logic
       to update the local membership list with the incoming one in both suspicion and non-suspicion.
    */
    private static final Logger logger = LoggerFactory.getLogger(MemberShipList.class);
    private ConcurrentHashMap<NodeId,NodeInfo> memberShipList;
    private Ring ring;
    private HyDFS hyDFS;

    public MemberShipList(Ring ring, HyDFS hyDFS){
        this.memberShipList = new ConcurrentHashMap<>();
        this.ring = ring;
        this.hyDFS = hyDFS;
    }

    public void addNodeEntry(NodeId nodeId,NodeInfo nodeInfo){
        logger.info("Adding node {} to the membership list", nodeId.getIp());
        memberShipList.put(nodeId,nodeInfo);
    }

    public void addAllNodeEntries(List<NodeId> nodeId, List<NodeInfo> nodeInfo) {
        if (nodeId.size() == nodeInfo.size()) {
            for (int i = 0; i < nodeId.size(); i++) {
                logger.info("Adding node {} to the membership list", nodeId.get(i).getIp());
                memberShipList.put(nodeId.get(i), nodeInfo.get(i));
            }
        }
    }

    public void removeNodeEntry(NodeId nodeId){
        logger.info("Removing the node {} from the membership list", nodeId.getIp());
        memberShipList.remove(nodeId);
    }

    public ConcurrentHashMap<NodeId,NodeInfo> getMemberShipList(){
        return this.memberShipList;
    }

    public String displaySuspects() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<NodeId, NodeInfo> entry : memberShipList.entrySet()) {
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();

            if (nodeInfo.getState().toString().equalsIgnoreCase("SUSPECT")) {
                sb.append(nodeId.getIp());
            }

        }

        return (sb.length() == 0 ? "No nodes currently suspected!" : sb.toString());
    }

    /*
     * Method for updating membership list for the reciever node -- FOR GOSSIP AND NO SUSPECT MODE (MP3 USED)
     * list: incoming membership list
     * reciever: currentNode that needs membership list updates
     */
    public void updateMembershipListNoSuspicion(Map<NodeId, NodeInfo> list, NodeId receiver){

        // go through each member in incoming membership list
        for(Map.Entry<NodeId,NodeInfo> entry: list.entrySet()){
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();
            NodeInfo currentNodeInfo = memberShipList.get(nodeId);

            // skip if we are looking at our own node or the node we are looking at is FAILED
            if(nodeId.equals(receiver) || nodeInfo.getState() == NodeInfo.State.FAILED) continue;

            // add if we dont have the node we are looking at
            if(currentNodeInfo == null){
                logger.info("Adding the node {} to the membership list", nodeId.getIp());
                memberShipList.put(nodeId, nodeInfo);
                ring.addNode(nodeId);
                hyDFS.rebalance();
            }
            //handle leave and remove if leave is there
            else if(nodeInfo.getState() == NodeInfo.State.LEAVE) {
                logger.info("Removing the node {} from the membership list for the incoming status LEAVE", nodeId.getIp());
                memberShipList.remove(nodeId);
                ring.removeNode(nodeId);
                hyDFS.rereplicate();
            }
            // update our node entry if the incoming is showing a more up to date heartbeat counter
            else{
                logger.info("Updating the current node {} in the membership list", nodeId.getIp());
                currentNodeInfo.updateNodeInfoNoSuspicion(nodeInfo);
                memberShipList.put(nodeId, currentNodeInfo);
            }
        }

        hyDFS.markInitialized();
    }

    /*
     * Method for updating membership list for the reciever node -- FOR GOSSIP AND SUSPECT MODE (MP3 IGNORE)
     * list: incoming membership list
     * reciever: currentNode that needs membership list updates
     */
    public void updateMembershipListWithSuspicion(Map<NodeId, NodeInfo> list, NodeId receiver){

        // go through each member in incoming membership list
        for(Map.Entry<NodeId, NodeInfo> entry: list.entrySet()){
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();
            NodeInfo currentNodeInfo = memberShipList.get(nodeId);

            if(nodeId.equals(receiver)){
                if(nodeInfo.getState() != NodeInfo.State.ALIVE){
                    logger.info("Updating the current node {} incarnation number and refuting the incoming suspicion", nodeId.getIp());
                    currentNodeInfo.setState(NodeInfo.State.ALIVE);
                    currentNodeInfo.setIncarnationNumber(currentNodeInfo.getIncarnationNumber() + 1);
                }
                continue;
            }

            if(nodeInfo.getState() == NodeInfo.State.FAILED) continue;

            // add if we dont have the node we are looking at
            if(currentNodeInfo == null){
                logger.info("Adding the node {} to the membership list", nodeId.getIp());
                memberShipList.put(nodeId, nodeInfo);
            }
            //handle leave and remove if leave is there
            else if(nodeInfo.getState() == NodeInfo.State.LEAVE) {
                logger.info("Removing the node {} from the membership list for the incoming status LEAVE", nodeId.getIp());
                memberShipList.remove(nodeId);
            }
            // update our node entry if the incoming is showing a more up to date heartbeat counter
            else{
                logger.info("Updating the current node {} in the membership list", nodeId.getIp());
                currentNodeInfo.updateNodeInfoSuspicion(nodeInfo);
                memberShipList.put(nodeId, currentNodeInfo);
            }
        }
    }

    /*
     * Method for updating membership list for the reciever node -- FOR PING AND SUSPECT MODE (MP3 IGNORE)
     * list: incoming membership list
     * reciever: currentNode that needs membership list updates
     * sender: the node that send the ping
     */
    public void updateMembershipListWithSuspicionPingMode(Map<NodeId, NodeInfo> list, NodeId receiver, NodeId sender){
        for(Map.Entry<NodeId, NodeInfo> entry: list.entrySet()){
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();
            NodeInfo currentNodeInfo = memberShipList.get(nodeId);

            // comparing the my own node in both membership lists (I know I am alive)
            if(nodeId.equals(receiver)){
                if(nodeInfo.getState() != NodeInfo.State.ALIVE){
                    currentNodeInfo.setState(NodeInfo.State.ALIVE);
                    currentNodeInfo.setIncarnationNumber(currentNodeInfo.getIncarnationNumber() + 1);
                }
                continue;
            }

            if (nodeInfo.getState() == NodeInfo.State.FAILED) {
                continue;
            }

            // if incoming membership list has that node as FAILED, mark the node in our list at FAILED
            // if(nodeInfo.getState() == NodeInfo.State.FAILED) {
            //     currentNodeInfo.setState(NodeInfo.State.FAILED);
            // };

            //handle leave and remove if leave is there
            if(currentNodeInfo == null){
                logger.info("Adding the node {} to the membership list", nodeId.getIp());
                memberShipList.put(nodeId, nodeInfo);
            }
            else if(nodeInfo.getState() == NodeInfo.State.LEAVE) {
                logger.info("Removing the node {} from the membership list for the incoming status LEAVE", nodeId.getIp());
                memberShipList.remove(nodeId);
            }
            else{
                logger.info("Updating the current node {} in the membership list", nodeId.getIp());
                currentNodeInfo.setState(nodeInfo.getState());
            }
        }
    }

    /*
     * Method for updating membership list for the reciever node -- FOR PING AND NO SUSPECT MODE (MP3 IGNORE)
     * list: incoming membership list
     * reciever: currentNode that needs membership list updates
     * sender: the node that send the ping
     */
    public void updateMembershipListNoSuspicionPingMode(Map<NodeId, NodeInfo> list, NodeId receiver, NodeId sender) {
        for(Map.Entry<NodeId, NodeInfo> entry: list.entrySet()){
            NodeInfo nodeInfo = entry.getValue();
            NodeId nodeId = entry.getKey();
            NodeInfo currentNodeInfo = memberShipList.get(nodeId);

            // comparing the my own node in both membership lists (I know I am alive)
            if(nodeId.equals(receiver) || nodeInfo.getState() == NodeInfo.State.FAILED) {
                // System.out.println("SKipping seeing my own node");
                continue;
            }
            // if incoming membership list has that node as FAILED, mark the node in our list at FAILED
            // if(currentNodeInfo != null && nodeInfo.getState() == NodeInfo.State.FAILED) {
            //     currentNodeInfo.setState(NodeInfo.State.FAILED);
            // };

            //handle leave and remove if leave is there
            if(currentNodeInfo == null){
                logger.info("Adding the node {} to the membership list", nodeId.getIp());
                memberShipList.put(nodeId, nodeInfo);
            }
            else if(nodeInfo.getState() == NodeInfo.State.LEAVE) {
                logger.info("Removing the node {} from the membership list for the incoming status LEAVE", nodeId.getIp());
                memberShipList.remove(nodeId);
            }
            else{
                logger.info("Updating the current node {} in the membership list", nodeId.getIp());
                currentNodeInfo.setState(nodeInfo.getState());
            }
        }
    }
}
