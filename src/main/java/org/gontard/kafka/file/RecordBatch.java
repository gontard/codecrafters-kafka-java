package org.gontard.kafka.file;

import java.util.List;

public record RecordBatch(List<Record> records) {
}
