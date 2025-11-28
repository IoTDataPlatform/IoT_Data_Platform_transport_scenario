package iot.data.platform.dump.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

public class FileDumpSinkConnector extends SinkConnector {
    public static final String FILE_PATH_CONFIG = "file.path";
    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileDumpSinkTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        if (!props.containsKey(FILE_PATH_CONFIG) || !props.get(FILE_PATH_CONFIG).endsWith(".ndjson")) {
            throw new IllegalArgumentException("file.path is required and must have format '.ndjson'");
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(new HashMap<>(configProps));
    }


    @Override
    public ConfigDef config() {
        return new ConfigDef().define(
                FILE_PATH_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Path to the file (created if missing). Output format: NDJSON."
        );
    }

}
