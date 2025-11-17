package iot.data.platform.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
    private final Class<T> type;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (u, data) -> {
            try {
                return data == null ? null : mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                try {
                    return (data == null || data.length == 0)
                            ? null
                            : mapper.readValue(data, type);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override public void close() {}
        };
    }
}
