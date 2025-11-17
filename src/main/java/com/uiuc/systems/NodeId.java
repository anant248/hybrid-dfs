package com.uiuc.systems;

import java.io.Serializable;
import java.util.Objects;

public class NodeId implements Serializable {
    /* This class builds the NodeId which contains IP, port and version/timestamp of when this was
       created. This is used as a key in the membership list map.
    */
    private String ip;
    private int port;
    private long version;

    public NodeId(){}
    public NodeId(String ip, int port, long version) {
        this.ip = ip;
        this.port = port;
        this.version = version;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public long getVersion() {
        return version;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeId nodeId = (NodeId) o;
        return port == nodeId.port && ip.equals(nodeId.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    @Override
    public String toString() {
        return "NodeId{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", version=" + version +
                '}';
    }
}
