package org.gontard.kafka;

import org.gontard.kafka.serde.Serde;

import java.nio.ByteBuffer;

public record RequestHeader(short apiKey,
                            short apiVersion,
                            int correlationId,
                            String clientId,
                            byte numberOfTaggedFields) {
    public static RequestHeader from(ByteBuffer buffer) {
        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();
        short clientIdLength = buffer.getShort();
        String clientId = Serde.getString(buffer, clientIdLength);
        byte numberOfTaggedFields = buffer.get();
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId, numberOfTaggedFields);
    }
}
