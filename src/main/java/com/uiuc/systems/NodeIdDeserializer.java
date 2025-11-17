package com.uiuc.systems;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import java.io.IOException;

public class NodeIdDeserializer extends KeyDeserializer {
    // This class is used to de-serialize the NodeId from a json string to a NodeId object which will be used by the receiver handler.
    @Override
    public Object deserializeKey(String key, DeserializationContext context) throws IOException {
        String[] parts = key.split(":");
        if (parts.length != 3) {
            throw new IOException("Invalid NodeId key: " + key);
        }
        String ip = parts[0];
        int port = Integer.parseInt(parts[1]);
        long version = Long.parseLong(parts[2]);
        return new NodeId(ip, port, version);
    }
}
