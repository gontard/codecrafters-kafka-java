package org.gontard.kafka;

import org.gontard.kafka.logs.Logging;
import org.gontard.kafka.serde.Serde;

import java.nio.ByteBuffer;
import java.util.List;

public class ApiVersionsApi implements KafkaApi, Logging {
    private final List<KafkaApi> apis;

    public ApiVersionsApi(List<KafkaApi> apis) {
        this.apis = apis;
    }

    @Override
    public byte apiKey() {
        return 18;
    }

    @Override
    public byte minVersion() {
        return 0;
    }

    @Override
    public byte maxVersion() {
        return 4;
    }

    @Override
    public void handle(ByteBuffer request, ByteBuffer response) {
        String clientId = Serde.getCompactString(request);
        String clientSoftwareVersion = Serde.getCompactString(request);
        byte tagBuffer = request.get();
        log("APIVersionsApi.handle: clientId: " + clientId + ", clientSoftwareVersion: " + clientSoftwareVersion + ", tagBuffer: " + tagBuffer);

        response.putShort(ErrorCode.NO_ERROR.getValue());
        response.put((byte) (apis.size() + 1)); // Use actual API count
        for (KafkaApi api : apis) {
            trace("API key: " + api.apiKey());
            response.putShort(api.apiKey());
            response.putShort(api.minVersion());
            response.putShort(api.maxVersion());
            response.put((byte) 0); // empty tag buffer
        }
        int throttleTime = 0;
        response.putInt(throttleTime);
        response.put((byte) 0); // empty tag buffer
    }
}
