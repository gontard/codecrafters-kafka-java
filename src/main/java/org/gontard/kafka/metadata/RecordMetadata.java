package org.gontard.kafka.metadata;

import java.util.UUID;

public sealed interface RecordMetadata permits TopicRecordMetadata, FeatureLevelRecordMetadata, PartitionRecordMetadata {
}

record TopicRecordMetadata(String topicName, UUID topicUuid) implements RecordMetadata {
}

record FeatureLevelRecordMetadata(String name, short featureLevel) implements RecordMetadata {
}

record PartitionRecordMetadata(int partitionId, UUID topicUuid) implements RecordMetadata {
}

