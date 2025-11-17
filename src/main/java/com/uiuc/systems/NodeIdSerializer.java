package com.uiuc.systems;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class NodeIdSerializer extends JsonSerializer<NodeId> {
    //This is used to serialize the NodeId as a string which will be used in the JSON that is sent over UDP
    @Override
    public void serialize(NodeId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        String key = value.getIp() + ":" + value.getPort() + ":" + value.getVersion();
        gen.writeFieldName(key);
    }
}

