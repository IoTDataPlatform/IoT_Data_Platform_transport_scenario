package iot.data.platform.dump.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Base64;

public class FileDumpSinkTask extends SinkTask {

    private final ObjectMapper mapper = new ObjectMapper();
    private BufferedWriter writer;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        String filePath = props.get(FileDumpSinkConnector.FILE_PATH_CONFIG);
        try {
            Path path = Path.of(filePath);
            Files.createDirectories(path.getParent() != null ? path.getParent() : Path.of("."));
            this.writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath, true), StandardCharsets.UTF_8
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file for writing: " + filePath, e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        for (SinkRecord rec : records) {
            try {
                Map<String, Object> line = new LinkedHashMap<>();

                line.put("topic", rec.topic());
                line.put("partition", rec.kafkaPartition());
                line.put("offset", rec.kafkaOffset());
                line.put("timestamp", rec.timestamp());

                Object normKey = normalize(rec.key());
                Object normValue = normalize(rec.value());

                line.put("key", normKey);
                line.put("value", normValue);

                writer.write(toJson(line));
                writer.newLine();
            } catch (Exception ex) {
                System.err.println("[FileDumpSink] Failed to process record: " + ex.getMessage());
            }
        }

        try {
            writer.flush();
        } catch (IOException e) {
            System.err.println("[FileDumpSink] Flush warning: " + e.getMessage());
        }
    }

    private String toJson(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    private Object normalize(Object value) {
        if (value == null) return null;

        if (value instanceof Struct s) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field f : s.schema().fields()) {
                Object v = s.get(f);
                map.put(f.name(), normalize(v));
            }
            return map;
        }

        if (value instanceof Map || value instanceof List
                || value instanceof String || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }

        if (value instanceof byte[] bytes) {
            Map<String, Object> b = new HashMap<>();
            b.put("encoding", "base64");
            b.put("data", Base64.getEncoder().encodeToString(bytes));
            return b;
        }
        if (value instanceof Byte[] boxed) {
            byte[] raw = new byte[boxed.length];
            for (int i = 0; i < boxed.length; i++) {
                raw[i] = boxed[i];
            }
            Map<String, Object> b = new HashMap<>();
            b.put("encoding", "base64");
            b.put("data", Base64.getEncoder().encodeToString(raw));
            return b;
        }

        return String.valueOf(value);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            if (writer != null) writer.flush();
        } catch (IOException e) {
            System.err.println("[FileDumpSink] Flush warning: " + e.getMessage());
        }
    }

    @Override
    public void stop() {
        try {
            if (writer != null) writer.close();
        } catch (IOException e) {
            System.err.println("[FileDumpSink] Close warning: " + e.getMessage());
        }
    }
}
