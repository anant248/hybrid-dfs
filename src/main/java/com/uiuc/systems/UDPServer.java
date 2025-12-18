package com.uiuc.systems;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPServer {
    /* This is a UDP Server class that receives the messages in the JSON format from all the other nodes.
       The JSON is then de-serialized to a Message object which is passed to the handlers.
    */
   private static final Logger logger = LoggerFactory.getLogger(UDPServer.class);
    private volatile boolean running = true;
    private DatagramSocket serverSocket;
    private static final int port = 6970;
    private ObjectMapper objectMapper;
    private Gossip gossip;
    private PingAck ping;

    private boolean suspicionActiveMechanism;
    private String protocolMode;
    private volatile double dropRate = 0.0; // default no drop

    public UDPServer(Gossip gossip, PingAck ping, boolean suspicionActiveMechanism, String protcolMode) {
        this.objectMapper = new ObjectMapper();
        this.gossip = gossip;
        this.ping = ping;
        this.suspicionActiveMechanism = suspicionActiveMechanism;
        this.protocolMode = protcolMode;
    }

    public boolean isSuspicionActiveMechanism() {
        return suspicionActiveMechanism;
    }

    public void setSuspicionActiveMechanism(boolean suspicionActiveMechanism) {
        this.suspicionActiveMechanism = suspicionActiveMechanism;
    }

    public void setDropRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            System.out.println("Drop rate must be between 0.0 and 1.0");
            return;
        }
        this.dropRate = rate;
        System.out.println("Drop rate set to " + dropRate);
    }

    private void handleIncomingRequests() {
        ExecutorService executor = null;
        try {
            //Using Executor service here to submit the de-serialization process to a worker thread
            executor = Executors.newCachedThreadPool();
            serverSocket = new DatagramSocket(port);
            byte[] incomingBytes = new byte[65535];
            DatagramPacket incomingPacket = new DatagramPacket(incomingBytes, incomingBytes.length);
            running = true;

            while (running) {
                serverSocket.receive(incomingPacket);

                // simulate drop
                if (Math.random() < dropRate) {
                    System.out.println("Dropping incoming packet from " + incomingPacket.getAddress() + ":" + incomingPacket.getPort());
                   logger.info("Dropping incoming packet from " + incomingPacket.getAddress() + ":" + incomingPacket.getPort());
                    continue; // skip processing this message
                }

                // otherwise process as usual
                executor.execute(() -> {
                    try {
                        Message msg = objectMapper.readValue(incomingPacket.getData(),0,incomingPacket.getLength(), Message.class);
                        if (protocolMode.equalsIgnoreCase("gossip")) {
                            //Handle this gossip now in Gossip protocol
                            if(suspicionActiveMechanism){
                                gossip.handleIncomingGossipsSuspicion(msg);
                            }
                            else{
                                gossip.handleIncomingGossips(msg);
                            }
                        } else if (protocolMode.equalsIgnoreCase("ping")) {
                            // Handle ping-ack now in PingAck Protocol
                            if(suspicionActiveMechanism){
                                ping.handleIncomingPingAckSuspicion(msg);
                            }
                            else{
                                ping.handleIncomingPingAckNoSuspicion(msg);
                            }
                        }
                    } catch (IOException e) {
                        if (running){
                           logger.info("An error occurred while processing the message: "+e.getMessage());
                            System.out.println("An error occurred while processing the message: "+e.getMessage());
                        }
                    }
                });
            }
        } catch (IOException e) {
         logger.error("An error occurred while handling incoming gossips: "+e.getMessage());
            System.out.println("An error occurred while handling incoming gossips: "+e.getMessage());
        } finally {
           logger.info("Shutting down the executor service");
            executor.shutdown();
        }
    }

    public void start(){
       logger.info("Starting up the UDP server on port {}", port);
        handleIncomingRequests();
    }

    public void stop() {
        running = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
           logger.warn("Shutting down the UDP Server on port {}", port);
            serverSocket.close();
        }
    }
}
