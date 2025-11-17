package com.uiuc.systems;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Map;

public class Message {
    /* This class builds the message object that will be communicated to the other nodes over UDP.
    */
    private String messageType;
    private NodeId node;
    @JsonDeserialize(keyUsing = NodeIdDeserializer.class)
    @JsonSerialize(keyUsing = NodeIdSerializer.class)
    private Map<NodeId, NodeInfo> memberShip;
    private String protocol;
    private Boolean suspicionMode;
    private Double dropRate;

    public Message(){}

    public Message(NodeId node, Map<NodeId, NodeInfo> memberShip) {
        this.node = node;
        this.memberShip = memberShip;
    }

    public String getMessageType() {
        return this.messageType;
    }

    public NodeId getNode() {
        return this.node;
    }

    public Map<NodeId, NodeInfo> getMemberShip() {
        return this.memberShip;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public Boolean getSuspicionMode() {
        return this.suspicionMode;
    }

    public Double getDropRate() {
        return this.dropRate;
    }

    public void setMessageType(String type) {
        this.messageType = type;
    }

    public void setNode(NodeId node) {
        this.node = node;
    }

    public void setMemberShip(Map<NodeId, NodeInfo> memberShip) {
        this.memberShip = memberShip;
    }


    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void disableSuspicionMode() {
        this.suspicionMode = false;
    }

    public void enableSuspicionMode() {
        this.suspicionMode = true;
    }

    public void setDropRate(Double dropRate) {
        this.dropRate = dropRate;
    }

    public String displayProtocol() {
        if (getSuspicionMode()) return getProtocol() + " , suspect";
        else return getProtocol() + " , nosuspect";
    }
}