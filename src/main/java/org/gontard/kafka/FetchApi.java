package org.gontard.kafka;

import org.gontard.kafka.file.TopicPartitionLogFile;
import org.gontard.kafka.logs.Logging;
import org.gontard.kafka.serde.Serde;
import org.gontard.kafka.topic.Partition;
import org.gontard.kafka.topic.Topic;
import org.gontard.kafka.topic.Topics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.gontard.kafka.ErrorCode.NO_ERROR;
import static org.gontard.kafka.ErrorCode.UNKNOWN_TOPIC;

public class FetchApi implements KafkaApi, Logging {
    private final Topics topics;

    public FetchApi(Topics topics) {
        this.topics = topics;
    }

    @Override
    public byte apiKey() {
        return 1;
    }

    @Override
    public byte minVersion() {
        return 0;
    }

    @Override
    public byte maxVersion() {
        return 17;
    }

    @Override
    public void handle(ByteBuffer request, ByteBuffer response) {
        log("Fetch");
        request.getInt(); // max_wait_ms
        request.getInt(); // min_bytes
        request.getInt(); // max_bytes
        request.get(); // isolation_level
        request.getInt(); // session_id
        request.getInt(); // session_epoch
        byte topicArrayLength = request.get();
        log("FetchApi.handle: topicArrayLength: " + (topicArrayLength - 1));
        List<RequestedTopic> requestedTopics = new ArrayList<>();
        for (int i = 0; i < topicArrayLength - 1; i++) {
            UUID topicUuid = Serde.getUuid(request); // topicId
            // partitions
            byte partitionsArrayLength = request.get();
            List<Integer> partitions = new ArrayList<>();
            for (int j = 0; j < partitionsArrayLength - 1; j++) {
                partitions.add(request.getInt());
                request.getLong(); // fetch_offset
                request.getInt(); // log_start_offset
                request.getInt(); // partition_max_bytes
            }
            requestedTopics.add(new RequestedTopic(topicUuid, partitions));
        }
        int throttleTime = 0;
        response.putInt(throttleTime);
        response.putShort(NO_ERROR.getValue());
        response.putInt(0); // sessionId

        response.put(topicArrayLength); // responses
        for (int i = 0; i < topicArrayLength - 1; i++) {
            RequestedTopic requestedTopic = requestedTopics.get(i);
            UUID topicUuid = requestedTopic.topicUuid();
            Optional<Topic> maybeTopic = topics.getTopic(topicUuid);
            if (maybeTopic.isPresent()) {
                Topic topic = maybeTopic.get();
                Serde.putUuid(response, topic.uuid());
                List<Partition> partitions = topic.partitions().stream().filter(partition -> requestedTopic.partitions().contains(partition.partitionId())).toList();
                response.put((byte) (partitions.size() + 1)); // partitions array length
                partitions.forEach(partition -> {
                    List<byte[]> recordBatches = loadRecordBatches(topic, partition);
                    log("FetchApi.handle: topicUuid: " + topicUuid + ", partition: " + partition.partitionId() + ", recordBatches: " + recordBatches.size());
                    response.putInt(partition.partitionId());
                    response.putShort(NO_ERROR.getValue());
                    response.putLong(0); // high_watermark
                    response.putLong(0); // last_stable_offset
                    response.putLong(0); // log_start_offset
                    response.put((byte) 1); // aborted_transactions array length
                    response.putInt(0); // preferred_read_replica
                    response.put((byte) (1 + recordBatches.size())); // records array length
                    recordBatches.forEach(response::put);
                    response.put((byte) 0); // tagBuffer
                });

            } else {
                log("FetchApi.handle: topicUuid: " + topicUuid + " not found");
                Serde.putUuid(response, topicUuid);
                response.put((byte) 2); // partitions array length
                response.putInt(0); // partition_index
                response.putShort(UNKNOWN_TOPIC.getValue());
                response.putLong(0); // high_watermark
                response.putLong(0); // last_stable_offset
                response.putLong(0); // log_start_offset
                response.put((byte) 1); // aborted_transactions array length
                response.putInt(0); // preferred_read_replica
                response.put((byte) 1); // records array length
                response.put((byte) 0); // tagBuffer
            }
            response.put((byte) 0); // tagBuffer
        }
        response.put((byte) 0); // tagBuffer
    }

    private List<byte[]> loadRecordBatches(Topic topic, Partition partition) {
        TopicPartitionLogFile logFile = TopicPartitionLogFile.get(topic.name(), partition.partitionId());
        try {
            return logFile.readRawBatches();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private record RequestedTopic(UUID topicUuid, List<Integer> partitions) {
    }
}
