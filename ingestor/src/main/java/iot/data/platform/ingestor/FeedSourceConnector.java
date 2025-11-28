package iot.data.platform.ingestor;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeedSourceConnector extends SourceConnector {
    public static final String CFG_FEED_NAME = "feed.name";
    public static final String CFG_FEED_URL = "feed.url";
    public static final String CFG_FEED_AGENCY = "feed.agency";
    public static final String CFG_TOPIC = "topic";
    public static final String CFG_INTERVAL_SEC = "interval.seconds";

    public static final String CFG_FETCH_PROFILE = "fetch.profile";
    public static final String CFG_FETCH_ACCEPT = "fetch.accept";
    public static final String CFG_FETCH_ACCEPT_ENCODING = "fetch.acceptEncoding";
    public static final String CFG_FETCH_HEADERS = "fetch.headers";
    public static final String CFG_FETCH_CONDITIONAL_GET = "fetch.conditionalGet";
    public static final String CFG_FETCH_DECOMPRESSION = "fetch.decompression";
    public static final String CFG_FETCH_TIMEOUT_MS = "fetch.timeout.ms";
    public static final String CFG_FETCH_USER_AGENT = "fetch.userAgent";

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FeedSourceTask.class;
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
                .define(CFG_FEED_NAME, ConfigDef.Type.STRING, "vehicle-positions", ConfigDef.Importance.HIGH, "Logical feed name")
                .define(CFG_FEED_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GTFS-RT VehiclePositions URL")
                .define(CFG_FEED_AGENCY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Agency name (e.g., SL, MTA)")
                .define(CFG_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target Kafka topic")
                .define(CFG_INTERVAL_SEC, ConfigDef.Type.INT, 10, ConfigDef.Importance.MEDIUM, "Poll interval, seconds")

                .define(CFG_FETCH_PROFILE, ConfigDef.Type.STRING, "default", ConfigDef.Importance.MEDIUM,
                        "Fetcher profile: default | trafiklab | custom")
                .define(CFG_FETCH_ACCEPT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Override Accept header")
                .define(CFG_FETCH_ACCEPT_ENCODING, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Override Accept-Encoding header")
                .define(CFG_FETCH_HEADERS, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Extra headers: \"K=V,K2:V2\" (comma/semicolon separated; '=' or ':')")
                .define(CFG_FETCH_CONDITIONAL_GET, ConfigDef.Type.BOOLEAN, null, ConfigDef.Importance.LOW,
                        "Use conditional GET (ETag/If-Modified-Since)")
                .define(CFG_FETCH_DECOMPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Decompression mode: AUTO|GZIP|DEFLATE|NONE|TRY_SMART")
                .define(CFG_FETCH_TIMEOUT_MS, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW,
                        "HTTP timeout in milliseconds")
                .define(CFG_FETCH_USER_AGENT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "User-Agent override");
    }
}
