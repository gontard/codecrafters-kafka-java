package org.gontard.kafka.logs;

import java.util.logging.Level;

public interface Logging {
    default void log(String message) {
        Logs.LOGGER.info(message);
    }

    default void trace(String message) {
        Logs.LOGGER.fine(message);
    }

    default void error(String message, Throwable e) {
        Logs.LOGGER.log(Level.SEVERE, message, e);
    }

    default void error(String message) {
        Logs.LOGGER.log(Level.SEVERE, message);
    }
}
