package org.gontard.kafka;

import org.gontard.kafka.file.TopicPartitionLogFile;
import org.gontard.kafka.logs.Logs;
import org.gontard.kafka.metadata.RecordMedataParser;
import org.gontard.kafka.metadata.RecordsMetadata;
import org.gontard.kafka.topic.Topics;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        int port = 9092;
        TopicPartitionLogFile clusterMetadata = TopicPartitionLogFile.get("__cluster_metadata", 0);
        try {
            RecordsMetadata recordMetadata = new RecordMedataParser(clusterMetadata.read()).parse();
            Logs.log("RecordsMetadata topics");
            Topics topics = recordMetadata.topics();
            Logs.log("Topics loaded " + topics);
            KafkaServer server = new KafkaServer(port, topics);
            Logs.log("Starting server on port " + port);
            server.start();
        } catch (IOException e) {
            Logs.error("Server error", e);
            System.exit(1);
        }
    }
}