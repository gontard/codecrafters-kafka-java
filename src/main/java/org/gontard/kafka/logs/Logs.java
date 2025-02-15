package org.gontard.kafka.logs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Logs {
    public static final Logger LOGGER = Logger.getLogger("my-kafka");
    private static final LogManager logManager = LogManager.getLogManager();

    static {
        try {
            logManager.readConfiguration(Logs.class.getResourceAsStream("/logging.properties"));
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Cannot read configuration file", exception);
        }
    }

    public static void log(String message) {
        Logs.LOGGER.info(message);
    }

    public static void trace(String message) {
        Logs.LOGGER.fine(message);
    }

    public static void error(String message, Throwable e) {
        Logs.LOGGER.log(Level.SEVERE, message, e);
    }

    public static void error(String message) {
        Logs.LOGGER.log(Level.SEVERE, message);
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(String.format("%02X ", bytes[i])); // Convert byte to 2-digit hex

            if ((i + 1) % 16 == 0) { // Wrap every 16 bytes
                sb.append("\n");
            }
        }
        return sb.toString().trim();
    }
}
