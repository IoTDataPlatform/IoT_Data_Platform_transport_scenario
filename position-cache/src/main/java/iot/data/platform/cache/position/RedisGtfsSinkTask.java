package iot.data.platform.cache.position;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class RedisGtfsSinkTask extends SinkTask {
    private Jedis jedis;
    private String keyPrefix;
    private int ttlSeconds;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        String host = props.getOrDefault(PositionCacheSinkConnector.REDIS_HOST, "redis");
        int port = Integer.parseInt(props.getOrDefault(PositionCacheSinkConnector.REDIS_PORT, "6379"));
        String password = props.get(PositionCacheSinkConnector.REDIS_PASSWORD);
        int db = Integer.parseInt(props.getOrDefault(PositionCacheSinkConnector.REDIS_DB, "600"));

        this.keyPrefix = props.getOrDefault(PositionCacheSinkConnector.KEY_PREFIX, "vehpos");
        this.ttlSeconds = Integer.parseInt(props.getOrDefault(PositionCacheSinkConnector.TTL_SECONDS, "0"));

        jedis = new Jedis(host, port);
        if (password != null && !password.isEmpty()) {
            jedis.auth(password);
        }
        if (db != 0) {
            jedis.select(db);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord rec : records) {
            try {
                Object val = rec.value();
                if (val == null) continue;

                Map<String, Object> fields;
                if (val instanceof Struct) {
                    fields = structToMap((Struct) val);
                } else if (val instanceof Map) {
                    fields = (Map<String, Object>) val;
                } else {
                    throw new ConnectException("Unsupported value type: " + val.getClass());
                }

                String agency = asString(fields.get("agency"));
                String vehicleId = asString(fields.get("vehicle_id"));
                String tripId = asString(fields.get("trip_id"));
                if (agency == null || vehicleId == null || tripId == null) {
                    continue;
                }

                String redisKey = keyPrefix + ":" + agency + ":" + tripId;

                Map<String, String> hash = new HashMap<>();
                for (Map.Entry<String, Object> e : fields.entrySet()) {
                    if (e.getValue() == null) continue;
                    hash.put(e.getKey(), toStringSafe(e.getValue()));
                }

                if (!hash.isEmpty()) {
                    try (Pipeline p = jedis.pipelined()) {
                        p.hset(redisKey, hash);
                        if (ttlSeconds > 0) {
                            p.expire(redisKey, ttlSeconds);
                        }
                        p.sync();
                    }
                }

            } catch (Exception e) {
                System.err.println("[RedisGtfsSink] Failed to process record: " + e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static Map<String, Object> structToMap(Struct s) {
        Map<String, Object> out = new HashMap<>();
        Schema schema = s.schema();
        schema.fields().forEach(f -> out.put(f.name(), s.get(f)));
        return out;
    }

    private static String asString(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }

    private static String toStringSafe(Object v) {
        if (v instanceof Double) {
            return String.format(Locale.ROOT, "%.7f", (Double) v);
        }
        if (v instanceof Float) {
            return String.format(Locale.ROOT, "%.7f", (Float) v);
        }
        return String.valueOf(v);
    }
}
