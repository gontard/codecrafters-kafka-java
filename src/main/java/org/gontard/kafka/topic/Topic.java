package org.gontard.kafka.topic;

import java.util.List;
import java.util.UUID;

public record Topic(String name, UUID uuid, List<Partition> partitions) {
}
