package org.gontard.kafka.metadata;

import org.gontard.kafka.topic.Partition;
import org.gontard.kafka.topic.Topic;
import org.gontard.kafka.topic.Topics;

import java.util.*;

public record RecordsMetadata(List<RecordMetadata> recordsMetadata) {
    public Topics topics() {
        List<Topic> topics = new ArrayList<>();
        Map<UUID, List<Partition>> partitionsByUuid = new HashMap<>();
        recordsMetadata.stream()
                .forEach(record -> {
                    if (record instanceof PartitionRecordMetadata(int partitionId, UUID topicUuid)) {
                        Partition partition = new Partition(partitionId);
                        partitionsByUuid.computeIfAbsent(topicUuid, k -> new ArrayList<>()).add(partition);
                    }
                });
        recordsMetadata.stream()
                .forEach(record -> {
                    if (record instanceof TopicRecordMetadata(String name, UUID uuid)) {
                        List<Partition> partitions = partitionsByUuid.get(uuid);
                        if (partitions == null) {
                            throw new IllegalArgumentException("No partition for topic " + name);
                        }
                        topics.add(new Topic(name, uuid, partitions));
                    }
                });
        return new Topics(topics);
    }
}
