package com.uiuc.systems;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UDPClient {
    /* This class builds a datagram socket to send the messages to all the available nodes other than the self node.
       This used Jackson library to convert/serialize the Message object to a JSON and send that over UDP.
       It uses ExecutorService in java to create worker threads as the sendMessage() tasks are submitted to it.
     */
   private static final Logger logger = LoggerFactory.getLogger(UDPClient.class);
    private static final int port = 6970;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private DatagramSocket clientSocket;

    public UDPClient () throws SocketException {
        clientSocket = new DatagramSocket();
    }

    public void sendMessage(String serverIp, Message msg){
        executorService.execute(() -> {
            try {
                String gossipMessageJson = new ObjectMapper().writeValueAsString(msg);
                byte[] jsonBytes = gossipMessageJson.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(jsonBytes, jsonBytes.length, InetAddress.getByName(serverIp), port);
                clientSocket.send(packet);

               logger.info("BANDWIDTH_LOG," + System.currentTimeMillis() + "," +
               msg.getNode().getIp() + "," +
               jsonBytes.length);
            }
            catch (IOException e) {
               logger.error("An error occurred while sending the datagram packet to {}", serverIp);
                System.out.println("[UDPClient] An error occurred while sending the datagram packet to "+ serverIp);
            }
        });
    }

    public void handleSend(ArrayList<String> ips, Message msg){
        for(String server: ips){
            sendMessage(server, msg);
        }
    }

    public void stop() {
        if (clientSocket != null && !clientSocket.isClosed()) {
           logger.warn("Closing the UDP client socket");
            clientSocket.close();
        }
    }
}
