package iot.data.platform.ingestor;

import com.google.transit.realtime.GtfsRealtime;
import iot.data.platform.ingestor.fetcher.GtfsRtHttpFetcher;
import iot.data.platform.ingestor.fetcher.ParamGtfsRtHttpFetcher;
import iot.data.platform.ingestor.fetcher.UrlProfile;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static iot.data.platform.ingestor.FeedSourceConnector.*;

public class FeedSourceTask extends SourceTask {
    private String name;
    private String url;
    private String topic;
    private String agency;
    private int intervalSeconds;

    private long nextAtMs;
    private GtfsRtHttpFetcher http;
    private UrlProfile urlProfile;

    private long lastHeaderTsSeen = -1L;
    private final Map<String, Long> lastEntityTsByVehicle = new HashMap<>();

    private static final Schema VEHICLE_POSITION_SCHEMA = SchemaBuilder.struct().name("gtfsrt.vehicle.position")
            .field("feed", Schema.STRING_SCHEMA)
            .field("agency", Schema.STRING_SCHEMA)
            .field("header_ts", Schema.INT64_SCHEMA)
            .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)

            .field("entity_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("trip_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("vehicle_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("vehicle_label", Schema.OPTIONAL_STRING_SCHEMA)

            .field("lat", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("lon", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("bearing", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("speed", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("odometer", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.name = props.getOrDefault(CFG_FEED_NAME, "vehicle-positions");
        this.url = Objects.requireNonNull(props.get(CFG_FEED_URL), "feed.url is required");
        this.topic = Objects.requireNonNull(props.get(CFG_TOPIC), "topic is required");
        this.agency = Objects.requireNonNull(props.get(CFG_FEED_AGENCY), "feed.agency is required");
        this.intervalSeconds = Integer.parseInt(props.getOrDefault(CFG_INTERVAL_SEC, "10"));

        this.http = new ParamGtfsRtHttpFetcher();

        String profile = props.getOrDefault(CFG_FETCH_PROFILE, "default").trim().toLowerCase(Locale.ROOT);
        UrlProfile base;
        if ("trafiklab".equals(profile)) {
            base = UrlProfile.trafiklab();
        } else {
            base = UrlProfile.defaultProfile();
        }

        String accept = props.getOrDefault(CFG_FETCH_ACCEPT, base.accept);
        String acceptEnc = props.getOrDefault(CFG_FETCH_ACCEPT_ENCODING, base.acceptEncoding);
        Map<String, String> headers = props.containsKey(CFG_FETCH_HEADERS) ?
                parseHeaders(props.get(CFG_FETCH_HEADERS)) : base.headers;
        boolean conditionalGet = props.get(CFG_FETCH_CONDITIONAL_GET) == null ?
                base.conditionalGet : Boolean.parseBoolean(props.get(CFG_FETCH_CONDITIONAL_GET));
        UrlProfile.Decompression dec = getOrDefaultDec(props.get(CFG_FETCH_DECOMPRESSION), base.decompression);
        Duration timeout = getOrDefaultTimeout(props.get(CFG_FETCH_TIMEOUT_MS), base.timeout);
        String userAgent = props.getOrDefault(CFG_FETCH_USER_AGENT, base.userAgent);

        this.urlProfile = new UrlProfile(accept, acceptEnc, headers, conditionalGet, dec, timeout, userAgent);

        this.nextAtMs = System.currentTimeMillis();

        try {
            Map<String, ?> off = context.offsetStorageReader().offset(Map.of("feed", name));
            if (off != null) {
                Object ts = off.get("lastHeaderTs");
                if (ts instanceof Number) lastHeaderTsSeen = ((Number) ts).longValue();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long nowMs = System.currentTimeMillis();
        if (nowMs < nextAtMs) {
            Thread.sleep(Math.min(1000L, nextAtMs - nowMs));
            return null;
        }

        final long callStartMs = System.currentTimeMillis();
        try {
            byte[] bytes = http.getBytes(url, urlProfile);
            if (bytes == null) {
                scheduleNext(callStartMs);
                return List.of();
            }

            GtfsRealtime.FeedMessage msg = GtfsRealtime.FeedMessage.parseFrom(bytes);

            long headerTs = msg.hasHeader() ? msg.getHeader().getTimestamp() : 0L;

            if (headerTs > 0 && headerTs == lastHeaderTsSeen) {
                scheduleNext(callStartMs);
                return List.of();
            }

            List<SourceRecord> out = new ArrayList<>();
            for (GtfsRealtime.FeedEntity ent : msg.getEntityList()) {
                if (!ent.hasVehicle()) continue;
                GtfsRealtime.VehiclePosition v = ent.getVehicle();

                if (!v.hasPosition()) continue;

                String vehicleId = v.hasVehicle() && v.getVehicle().hasId() ? v.getVehicle().getId() : null;
                Long entityTs = v.hasTimestamp() ? v.getTimestamp() : null;
                if (vehicleId != null && entityTs != null) {
                    Long last = lastEntityTsByVehicle.get(vehicleId);
                    if (entityTs.equals(last)) continue;
                }

                Struct s = buildStruct(headerTs, ent, v);

                long keyTs = (entityTs != null && entityTs > 0) ? entityTs
                        : (headerTs > 0 ? headerTs : Instant.now().getEpochSecond());
                String idPart = (vehicleId != null ? vehicleId : (ent.hasId() ? ent.getId() : "unknown"));
                String key = name + "|" + idPart + "|" + keyTs;

                Map<String, String> sourcePartition = Map.of("feed", name);
                Map<String, Object> sourceOffset = new HashMap<>();
                if (headerTs > 0) sourceOffset.put("lastHeaderTs", headerTs);
                sourceOffset.put("lastFetchMs", System.currentTimeMillis());

                out.add(new SourceRecord(
                        sourcePartition, sourceOffset,
                        topic, null,
                        Schema.STRING_SCHEMA, key,
                        VEHICLE_POSITION_SCHEMA, s
                ));

                if (vehicleId != null && entityTs != null) {
                    lastEntityTsByVehicle.put(vehicleId, entityTs);
                }
            }

            if (headerTs > 0) lastHeaderTsSeen = headerTs;

            scheduleNext(callStartMs);
            return out;
        } catch (Exception e) {
            scheduleNext(callStartMs);
            System.err.println("[VehiclePositions] " + e.getMessage());
            return List.of();
        }
    }

    private Struct buildStruct(long headerTs, GtfsRealtime.FeedEntity ent, GtfsRealtime.VehiclePosition v) {
        GtfsRealtime.TripDescriptor trip = v.hasTrip() ? v.getTrip() : null;
        GtfsRealtime.VehicleDescriptor veh = v.hasVehicle() ? v.getVehicle() : null;
        GtfsRealtime.Position pos = v.getPosition();

        Float lat = pos.hasLatitude() ? pos.getLatitude() : null;
        Float lon = pos.hasLongitude() ? pos.getLongitude() : null;
        Float bearing = pos.hasBearing() ? pos.getBearing() : null;
        Float speed = pos.hasSpeed() ? pos.getSpeed() : null;
        Double odometer = pos.hasOdometer() ? pos.getOdometer() : null;

        return new Struct(VEHICLE_POSITION_SCHEMA)
                .put("feed", name)
                .put("agency", agency)
                .put("header_ts", headerTs)
                .put("timestamp", v.hasTimestamp() ? v.getTimestamp() : null)

                .put("entity_id", ent.hasId() ? ent.getId() : null)
                .put("trip_id", (trip != null && trip.hasTripId()) ? trip.getTripId() : null)
                .put("vehicle_id", (veh != null && veh.hasId()) ? veh.getId() : null)
                .put("vehicle_label", (veh != null && veh.hasLabel()) ? veh.getLabel() : null)

                .put("lat", lat)
                .put("lon", lon)
                .put("bearing", bearing)
                .put("speed", speed)
                .put("odometer", odometer);
    }

    private void scheduleNext(long callStartMs) {
        long elapsed = Math.max(1, System.currentTimeMillis() - callStartMs);
        long sleepMs = Math.max(50L, intervalSeconds * 1000L - elapsed);
        nextAtMs = System.currentTimeMillis() + sleepMs;
    }

    @Override
    public void stop() {
    }

    private static Map<String, String> parseHeaders(String raw) {
        if (raw == null || raw.isBlank()) return Map.of();
        Map<String, String> res = new LinkedHashMap<>();
        String[] pairs = raw.split("[,;]");
        for (String p : pairs) {
            String[] kv = p.split("[:=]", 2);
            if (kv.length == 2) {
                String k = kv[0].trim();
                String v = kv[1].trim();
                if (!k.isEmpty()) res.put(k, v);
            }
        }
        return res;
    }

    private static UrlProfile.Decompression getOrDefaultDec(String val, UrlProfile.Decompression def) {
        if (val == null || val.isBlank()) return def;
        try {
            return UrlProfile.Decompression.valueOf(val.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return def;
        }
    }

    private static Duration getOrDefaultTimeout(String ms, Duration def) {
        if (ms == null || ms.isBlank()) return def;
        try {
            long v = Long.parseLong(ms.trim());
            return Duration.ofMillis(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
