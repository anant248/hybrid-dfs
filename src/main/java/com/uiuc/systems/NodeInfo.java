package com.uiuc.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeInfo {
    /*
      This class maintains the node's state, lastUpdated timestamp, incarnation number in case of
      suspicion mode, heartbeat counter in case of gossip protocol. A node's last suspected and failed
      timestamp is also stored to detect failures while checking for timeouts.
     */

   private static final Logger logger = LoggerFactory.getLogger(NodeInfo.class);
    enum State { ALIVE, SUSPECT, FAILED, LEAVE };
    private State state;
    private long lastUpdated;
    private long suspectTime;
    private long failTime;
    private long incarnationNumber;
    private long heartbeatCounter=0;

    public NodeInfo(){}

    public NodeInfo(State state, long lastUpdated, long heartbeatCounter) {
        this.state = state;
        this.lastUpdated = lastUpdated;
        this.heartbeatCounter = heartbeatCounter;
    }

    public NodeInfo(State state, long lastUpdated, long suspectTime, long failTime, long incarnationNumber, long heartbeatCounter) {
        this.state = state;
        this.lastUpdated = lastUpdated;
        this.suspectTime = suspectTime;
        this.failTime = failTime;
        this.incarnationNumber = incarnationNumber;
        this.heartbeatCounter = heartbeatCounter;
    }

    public long getHeartbeatCounter() {
        return heartbeatCounter;
    }

    public void setHeartbeatCounter(long heartbeatCounter) {
        this.heartbeatCounter = heartbeatCounter;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public long getIncarnationNumber() {
        return incarnationNumber;
    }

    public void setIncarnationNumber(long incarnationNumber) {
        this.incarnationNumber = incarnationNumber;
    }

    public long getSuspectTime() {
        return suspectTime;
    }

    public void setSuspectTime(long suspectTime) {
        this.suspectTime = suspectTime;
    }

    public long getFailTime() {
        return failTime;
    }

    public void setFailTime(long failTime) {
        this.failTime = failTime;
    }

    public void updateNodeInfoNoSuspicion(NodeInfo info){
        long currNodeHeartbeatCounter = this.heartbeatCounter;
        State incomingNodeState = info.getState();
        long incomingNodeHeartbeatCounter = info.getHeartbeatCounter();

        if(incomingNodeHeartbeatCounter > currNodeHeartbeatCounter){
            setHeartbeatCounter(incomingNodeHeartbeatCounter);
            setLastUpdated(System.currentTimeMillis());
            setState(incomingNodeState);
        }
       logger.info("Updated the current node state in the membership list to {}", getState());
    }

    public void updateNodeInfoSuspicion(NodeInfo info){
        long currNodeHeartbeatCounter = this.heartbeatCounter;
        long currNodeIncarnationNumber = this.incarnationNumber;
        State currentNodeState = this.state;
        State incomingNodeState = info.getState();
        long incomingNodeLastSeen = info.getLastUpdated();
        long incomingNodeIncarnationNumber = info.getIncarnationNumber();
        long incomingNodeHeartbeatCounter = info.getHeartbeatCounter();

        if(incomingNodeIncarnationNumber > currNodeIncarnationNumber){
            setIncarnationNumber(incomingNodeIncarnationNumber);
            setState(incomingNodeState);
            setHeartbeatCounter(incomingNodeHeartbeatCounter);
            setLastUpdated(incomingNodeLastSeen);
           logger.info("Updated the current node with newer incarnation: inc={}, heartbeat={}, state={}, last updated={}", getIncarnationNumber(), getHeartbeatCounter(), getState(), getLastUpdated());
        }
        else if(incomingNodeIncarnationNumber == currNodeIncarnationNumber){
            if(incomingNodeState == State.SUSPECT && currentNodeState == State.ALIVE){
                setState(State.SUSPECT);
            }
            if(incomingNodeHeartbeatCounter > currNodeHeartbeatCounter){
                setHeartbeatCounter(incomingNodeHeartbeatCounter);
                setLastUpdated(System.currentTimeMillis());
                setState(incomingNodeState);
            }
           logger.info("Updated the current node state in the membership list to {}", getState());
        }
    }
}

