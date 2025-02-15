package org.gontard.kafka.topic;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public record Topics(List<Topic> topics) {
    public Optional<Topic> getTopic(String topicName) {
        return topics.stream()
                .filter(topic -> topic.name().equals(topicName))
                .findFirst();
    }

    public Optional<Topic> getTopic(UUID topicUuid) {
        return topics.stream()
                .filter(topic -> topic.uuid().equals(topicUuid))
                .findFirst();
    }
}
