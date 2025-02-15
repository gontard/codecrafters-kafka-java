package org.gontard.kafka;

import org.gontard.kafka.logs.Logging;
import org.gontard.kafka.serde.Serde;
import org.gontard.kafka.topic.Topic;
import org.gontard.kafka.topic.Topics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.gontard.kafka.ErrorCode.NO_ERROR;
import static org.gontard.kafka.ErrorCode.UNKNOWN_TOPIC_OR_PARTITION;

public class DescribeTopicPartitionsApi implements KafkaApi, Logging {
    private static final UUID NULL_UUID = new UUID(0, 0);

    private final Topics topics;

    public DescribeTopicPartitionsApi(Topics topics) {
        this.topics = topics;
    }

    @Override
    public byte apiKey() {
        return 75;
    }

    @Override
    public byte minVersion() {
        return 0;
    }

    @Override
    public byte maxVersion() {
        return 0;
    }

    @Override
    public void handle(ByteBuffer request, ByteBuffer response) {
        byte topicArrayLength = request.get();
        List<String> requestedTopics = new ArrayList<>();
        for (int i = 0; i < topicArrayLength - 1; i++) {
            requestedTopics.add(Serde.getCompactString(request));
            request.get(); // tagBuffer
        }
        int responsePartitionLimit = request.getInt();
        byte cursor = request.get();
        byte tagBuffer = request.get();
        log("DescribeTopicPartitionsApi.handle: topics: " + requestedTopics + ", responsePartitionLimit: " + responsePartitionLimit + ", cursor: " + cursor + ", tagBuffer: " + tagBuffer);

        int throttleTime = 0;
        response.putInt(throttleTime);
        response.put(topicArrayLength);
        for (int i = 0; i < topicArrayLength - 1; i++) {
            String topicName = requestedTopics.get(i);
            Optional<Topic> maybeTopic = topics.getTopic(topicName);
            if (maybeTopic.isPresent()) {
                Topic topic = maybeTopic.get();
                response.putShort(NO_ERROR.getValue());
                Serde.putCompactString(response, topic.name());
                Serde.putUuid(response, topic.uuid());
                response.put((byte) 0); // isInternal
                byte partitionArrayLength = (byte) (topic.partitions().size() + 1);
                response.put(partitionArrayLength);
                topic.partitions().forEach(partition -> {
                    response.putShort(NO_ERROR.getValue());
                    response.putInt(partition.partitionId()); //  Partition Index
                    response.putInt(1); // Leader ID
                    response.putInt(0); // Leader Epoch
                    response.put((byte) 2); // Replica Nodes - Array Length (1)
                    response.putInt(1); // Replica Nodes - First Replica Node
                    response.put((byte) 2); // ISR Nodes - Array Length (1)
                    response.putInt(1); // ISR Node - First ISR Node
                    response.put((byte) 1); // Eligible Leader Replicas (empty)
                    response.put((byte) 1); // Last Known ELR (empty)
                    response.put((byte) 1); // Offline Replicas (empty)
                    response.put((byte) 0); // tagBuffer
                });
            } else {
                log("Topic not found: " + topicName);
                response.putShort(UNKNOWN_TOPIC_OR_PARTITION.getValue());
                Serde.putCompactString(response, topicName);
                Serde.putUuid(response, NULL_UUID);
                response.put((byte) 0); // isInternal
                response.put((byte) 1); // partitionArrayLength
            }
            // https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
            int authorizedOperations = 0x00000df8; // 0000 1101 1111 1000
            response.putInt(authorizedOperations);
            response.put((byte) 0); // tagBuffer
        }
        byte nextCursor = (byte) 0xff;
        response.put(nextCursor);
        response.put((byte) 0); // tagBuffer
    }
}
