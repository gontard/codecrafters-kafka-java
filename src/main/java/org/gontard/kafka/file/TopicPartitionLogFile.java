package org.gontard.kafka.file;

import org.gontard.kafka.logs.Logging;
import org.gontard.kafka.serde.Serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TopicPartitionLogFile implements Logging {
    private final Path path;

    public TopicPartitionLogFile(Path path) {
        this.path = path;
    }

    public static TopicPartitionLogFile get(String topicName, int partition) {
        String fileName = String.format("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partition);
        return new TopicPartitionLogFile(Paths.get(fileName));
    }

    public List<RecordBatch> read() throws IOException {
        byte[] fileContent = getFileContent();
        ByteBuffer buffer = ByteBuffer.wrap(fileContent);

        List<RecordBatch> recordBatches = new ArrayList<>();
        while (buffer.hasRemaining()) {
            long baseOffset = buffer.getLong(); // Base Offset
            int batchLength = buffer.getInt();// Batch Length
            buffer.getInt(); // Partition Leader Epoch
            buffer.get(); // Magic Byte
            buffer.getInt(); // CRC
            buffer.getShort(); // Attributes
            buffer.getInt(); // Last Offset Delta
            buffer.getLong(); // Base Timestamp
            buffer.getLong(); // Max Timestamp
            buffer.getLong(); // Producer ID
            buffer.getShort(); // Producer Epoch
            buffer.getInt(); // Base Sequence
            int recordsCount = buffer.getInt();
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < recordsCount; i++) {
                int recordLength = Serde.getSignedVariableInt(buffer);// Record length
                buffer.get(); // Attributes
                Serde.getSignedVariableInt(buffer); // Timestamp Delta
                Serde.getSignedVariableInt(buffer); // Offset Delta
                int keyLength = Serde.getSignedVariableInt(buffer);
                if (keyLength != -1) { // Key Length => Null
                    throw new IllegalArgumentException("Key Length must be -1 " + keyLength);
                }

                Record record = readRecord(buffer);
                records.add(record);
            }
            recordBatches.add(new RecordBatch(records));
        }
        return recordBatches;
    }

    public List<byte[]> readRawBatches() throws IOException {
        byte[] fileContent = getFileContent();
        ByteBuffer buffer = ByteBuffer.wrap(fileContent);

        List<byte[]> recordBatches = new ArrayList<>();
        while (buffer.hasRemaining()) {
            buffer.getLong(); // Base Offset
            int batchLength = buffer.getInt();// Batch Length
            byte[] batch = new byte[batchLength + 12];
            buffer.position(buffer.position() - 12);
            buffer.get(batch);
            recordBatches.add(batch);
        }
        return recordBatches;
    }

    private byte[] getFileContent() throws IOException {
        log("Reading file: " + path);
        return Files.readAllBytes(path);
    }

    private Record readRecord(ByteBuffer buffer) {
        int length = Serde.getSignedVariableInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        if (buffer.get() != 0) { // tagBuffer
            throw new IllegalArgumentException("Header array count must be 0");
        }
        return new Record(bytes);
    }
}
