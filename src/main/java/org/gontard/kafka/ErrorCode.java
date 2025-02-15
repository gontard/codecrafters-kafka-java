package org.gontard.kafka;

public enum ErrorCode {
    NO_ERROR(0),
    UNKNOWN_TOPIC_OR_PARTITION(3),
    UNSUPPORTED_VERSION(35),
    INVALID_REQUEST(42),
    UNKNOWN_TOPIC(100);

    private final short value;

    ErrorCode(int value) {
        this.value = (short) value;
    }

    public short getValue() {
        return value;
    }
}
