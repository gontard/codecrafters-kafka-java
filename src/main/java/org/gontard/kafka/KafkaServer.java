package org.gontard.kafka;

import org.gontard.kafka.logs.Logging;
import org.gontard.kafka.topic.Topics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaServer implements Logging {
    private final int port;
    private final ExecutorService clientExecutor = Executors.newCachedThreadPool();
    private final List<KafkaApi> apis;

    public KafkaServer(int port, Topics topics) {
        this.port = port;
        this.apis = initializeApis(topics);
    }

    private List<KafkaApi> initializeApis(Topics topics) {
        List<KafkaApi> apis = new ArrayList<>();
        ApiVersionsApi apiVersionsApi = new ApiVersionsApi(apis);
        Collections.addAll(apis, new FetchApi(topics), apiVersionsApi, new DescribeTopicPartitionsApi(topics));
        return apis;
    }

    public void start() throws IOException {
        log("Starting Kafka server on port " + port);
        AtomicBoolean isRunning = new AtomicBoolean(true);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (isRunning.get()) {
                log("Waiting for client connection...");
                Socket clientSocket = serverSocket.accept();
                log("New client connected");
                clientExecutor.execute(() -> {
                    try {
                        handleClient(clientSocket);
                    } catch (Exception e) {
                        error("Error handling client", e);
                        isRunning.set(false);
                    }
                });
            }
        }
    }

    private void handleClient(Socket clientSocket) throws IOException {
        ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
        boolean isRunning = true;
        try (
                clientSocket;
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream()
        ) {
            while (isRunning) {
                log("Reading message...");
                try {
                    ByteBuffer requestBuffer = readRequest(inputStream);
                    handleRequest(requestBuffer, responseBuffer);
                    writeResponse(responseBuffer, outputStream);
                } catch (BufferUnderflowException e) {
                    error("Error reading request", e);
                    isRunning = false;
                }
            }
        }
    }

    private void handleRequest(ByteBuffer requestBuffer, ByteBuffer responseBuffer) {
        RequestHeader requestHeader = RequestHeader.from(requestBuffer);
        responseBuffer.putInt(requestHeader.correlationId());
        if (requestHeader.apiKey() != 18) {
            responseBuffer.put((byte) 0); // empty tag buffer
        }
        findApi(requestHeader).ifPresentOrElse(
                (KafkaApi api) -> api.handle(requestBuffer, responseBuffer),
                () -> {
                    error("Unsupported API version");
                    responseBuffer.putShort(ErrorCode.UNSUPPORTED_VERSION.getValue());
                }
        );
    }

    private Optional<KafkaApi> findApi(RequestHeader requestHeader) {
        return apis.stream()
                .filter(api -> api.apiKey() == requestHeader.apiKey())
                .filter(api -> requestHeader.apiVersion() >= api.minVersion())
                .filter(api -> requestHeader.apiVersion() <= api.maxVersion())
                .findAny();
    }

    private ByteBuffer readRequest(InputStream inputStream) throws IOException {
        int messageSize = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
        trace("Message size: " + messageSize);
        return ByteBuffer.wrap(inputStream.readNBytes(messageSize));
    }

    private void writeResponse(ByteBuffer responseBuffer,
                               OutputStream outputStream) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        responseBuffer.flip();
        int length = responseBuffer.remaining();
        lengthBuffer.putInt(length);

        outputStream.write(lengthBuffer.array());
        outputStream.write(responseBuffer.array(), responseBuffer.position(), length);
        outputStream.flush();
        trace("Response sent - length " + length);

        responseBuffer.clear();
        lengthBuffer.clear();
    }
}
