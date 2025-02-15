package org.gontard.kafka.serde;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Serde {

    public static String getCompactString(ByteBuffer buffer) {
        byte length = buffer.get();
        return getString(buffer, length - 1);
    }

    public static void putCompactString(ByteBuffer buffer, String value) {
        buffer.put((byte) (value.length() + 1));
        buffer.put(value.getBytes());
    }

    public static String getString(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    public static UUID getUuid(ByteBuffer buffer) {
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public static void putUuid(ByteBuffer buffer, UUID value) {
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
    }

    public static int getSignedVariableInt(ByteBuffer buffer) {
        int value = getUnsignedVariableInt(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    public static int getUnsignedVariableInt(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw new IllegalArgumentException("getUnsignedVarint");
        }
        value |= b << i;
        return value;
    }
}
