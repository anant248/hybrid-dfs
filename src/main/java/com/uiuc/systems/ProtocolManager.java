package com.uiuc.systems;

public class ProtocolManager {
    /* This class methods will be called directly by the Main class and is used to route the requests
       to appropriate protocol(Gossip or PingAck) with the suspicion/no-suspicion flag.
       This class contains startProtocol() and stopProtocol() methods that will invoke the scheduler.
    */
    private Gossip gossip;
    private PingAck ping;
    private MemberShipScheduler scheduler;
    private String activeMode;
    private boolean suspicionFlag;
    private long protocolInterval;

    public ProtocolManager(Gossip gossip, PingAck ping, MemberShipScheduler scheduler, long protocolInterval, UDPClient udpClient, UDPServer udpServer) {
        this.gossip = gossip;
        this.ping = ping;
        this.scheduler = scheduler;
        this.protocolInterval = protocolInterval;
        this.activeMode = null;
        this.suspicionFlag = false;
    }


    public void switchProtocol(String mode, boolean suspicion) {
        // if switching to the protocol already running
        if (activeMode != null && activeMode.equals(mode) && suspicionFlag == suspicion) {
            System.out.println("No change made since the current mode is already " + activeMode + "with suspicion set to " + suspicionFlag);
            return;
        }

        // stop existing running protocol
        System.out.println("Stopping exisiting protocol: " + this.activeMode + " and suspect mode is set to " + this.suspicionFlag);
        stopProtocol();

        this.activeMode = mode;
        this.suspicionFlag = suspicion;

        System.out.println("Switching protocol to: " + this.activeMode + " and suspect mode is set to " + this.suspicionFlag);

        if ("gossip".equalsIgnoreCase(this.activeMode)) {

            // start gossiping
            startProtocol(() -> gossip.sendGossip(this.suspicionFlag));
        } else if ("ping".equalsIgnoreCase(this.activeMode)) {

            // start pinging
            startProtocol(() -> ping.sendPing(this.suspicionFlag));
        }
    }

    public void startProtocol(Runnable task) {
        scheduler.start(task, protocolInterval);
    }

    public void stopProtocol() {
        scheduler.stop();
    }

    public void startUDPServer(Runnable server) {
        new Thread(server, "UDPServerThread").start();
    }


    public Boolean getSuspicionMode() {
        return this.suspicionFlag;
    }

    public String getActiveMode() {
        return this.activeMode;
    }
}