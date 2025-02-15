package org.gontard.kafka.metadata;

import org.gontard.kafka.file.Record;
import org.gontard.kafka.file.RecordBatch;
import org.gontard.kafka.serde.Serde;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class RecordMedataParser {
    private final List<RecordBatch> recordBatches;

    public RecordMedataParser(List<RecordBatch> recordBatches) {
        this.recordBatches = recordBatches;
    }

    public RecordsMetadata parse() {
        return new RecordsMetadata(records().map(this::parseMetadata).collect(toList()));
    }

    private Stream<Record> records() {
        return recordBatches.stream().flatMap(batch -> batch.records().stream());
    }

    private RecordMetadata parseMetadata(Record record) {
        ByteBuffer buffer = ByteBuffer.wrap(record.data());
        buffer.get(); // Frame Version
        byte type = buffer.get();
        buffer.get(); // Version
        UUID topicUuid;
        RecordMetadata metadata;
        switch (type) {
            case 2: // Topic record
                String topicName = Serde.getCompactString(buffer); // Key
                topicUuid = Serde.getUuid(buffer);
                metadata = new TopicRecordMetadata(topicName, topicUuid);
                break;
            case 3: // Partition record
                int partitionId = buffer.getInt();
                topicUuid = Serde.getUuid(buffer);

                readReplicasArray(buffer); // Replicas
                readReplicasArray(buffer); // In Sync Replicas
                readReplicasArray(buffer); // Removing Replicas
                readReplicasArray(buffer); // Adding Replicas

                buffer.getInt(); // Replica ID of the leader
                buffer.getInt(); // Leader Epoch
                buffer.getInt(); // Partition Epoch
                byte directoriesArrayLength = buffer.get();
                for (int j = 0; j < directoriesArrayLength - 1; j++) {
                    Serde.getUuid(buffer); // Directory UUID
                }
                metadata = new PartitionRecordMetadata(partitionId, topicUuid);
                break;
            case 12: // Feature level record
                String name = Serde.getCompactString(buffer); // Key
                metadata = new FeatureLevelRecordMetadata(name, buffer.getShort());
                break;
            default:
                throw new IllegalArgumentException("Unknown record type: " + type);
        }
        if (buffer.get() != 0) { // tagBuffer
            throw new IllegalArgumentException("Record tag buffer must be 0");
        }
        return metadata;
    }

    private void readReplicasArray(ByteBuffer buffer) {
        int arrayLength = Serde.getUnsignedVariableInt(buffer);
        for (int j = 0; j < arrayLength - 1; j++) {
            buffer.getInt(); // Replica ID
        }
    }
}
