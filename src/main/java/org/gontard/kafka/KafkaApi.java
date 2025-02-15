package org.gontard.kafka;

import java.nio.ByteBuffer;

public interface KafkaApi {
    byte apiKey();

    byte minVersion();

    byte maxVersion();

    void handle(ByteBuffer request, ByteBuffer response);
}
