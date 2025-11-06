package iot.data.platform.cache.position;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PositionCacheSinkConnector extends SinkConnector {
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_DB = "redis.db";
    public static final String KEY_PREFIX = "redis.key.prefix";
    public static final String TTL_SECONDS = "ttl.seconds";

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisGtfsSinkTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
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
        return new ConfigDef()
                .define(REDIS_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Redis host")
                .define(REDIS_PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Redis port")
                .define(REDIS_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.MEDIUM, "Redis password (optional)")
                .define(REDIS_DB, ConfigDef.Type.INT, ConfigDef.Importance.LOW, "Redis DB index")
                .define(KEY_PREFIX, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "Key prefix for vehicle position hashes")
                .define(TTL_SECONDS, ConfigDef.Type.INT, ConfigDef.Importance.LOW, "TTL for vehicle position keys in seconds (0=disabled)");
    }
}
