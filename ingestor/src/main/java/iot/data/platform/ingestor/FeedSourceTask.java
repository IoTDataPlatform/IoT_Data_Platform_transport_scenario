package iot.data.platform.ingestor;

import com.google.transit.realtime.GtfsRealtime;
import iot.data.platform.avro.VehiclePositionSchemas;
import iot.data.platform.ingestor.fetcher.GtfsRtHttpFetcher;
import iot.data.platform.ingestor.fetcher.ParamGtfsRtHttpFetcher;
import iot.data.platform.ingestor.fetcher.UrlProfile;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.time.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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

    private static final AvroData AVRO_DATA = new AvroData(0);

    private static final ZoneId BUSINESS_DAY_ZONE = ZoneId.of("UTC+1");

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

        String profile = props.getOrDefault(CFG_FETCH_PROFILE, "default")
                .trim()
                .toLowerCase(Locale.ROOT);
        UrlProfile base;
        if ("trafiklab".equals(profile)) {
            base = UrlProfile.trafiklab();
        } else {
            base = UrlProfile.defaultProfile();
        }

        String accept = props.getOrDefault(CFG_FETCH_ACCEPT, base.accept);
        String acceptEnc = props.getOrDefault(CFG_FETCH_ACCEPT_ENCODING, base.acceptEncoding);
        Map<String, String> headers = props.containsKey(CFG_FETCH_HEADERS)
                ? parseHeaders(props.get(CFG_FETCH_HEADERS))
                : base.headers;
        boolean conditionalGet = props.get(CFG_FETCH_CONDITIONAL_GET) == null
                ? base.conditionalGet
                : Boolean.parseBoolean(props.get(CFG_FETCH_CONDITIONAL_GET));
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

            List<SourceRecord> out = new java.util.ArrayList<>();
            for (GtfsRealtime.FeedEntity ent : msg.getEntityList()) {
                if (!ent.hasVehicle()) continue;
                GtfsRealtime.VehiclePosition v = ent.getVehicle();

                if (!v.hasPosition()) continue;

                String vehicleId = v.hasVehicle() && v.getVehicle().hasId()
                        ? v.getVehicle().getId()
                        : null;

                Long entityTs = v.hasTimestamp() ? v.getTimestamp() : null;
                if (vehicleId != null && entityTs != null) {
                    Long last = lastEntityTsByVehicle.get(vehicleId);
                    if (entityTs.equals(last)) continue;
                }

                GenericRecord record = buildRecord(headerTs, ent, v);

                SchemaAndValue sav = AVRO_DATA.toConnectData(
                        VehiclePositionSchemas.VEHICLE_POSITION_SCHEMA,
                        record
                );

                long keyTs = computeKeyTs(headerTs, entityTs);
                String idPart = (vehicleId != null
                        ? vehicleId
                        : (ent.hasId() ? ent.getId() : "unknown"));
                String key = name + "|" + idPart + "|" + keyTs;

                Map<String, String> sourcePartition = Map.of("feed", name);
                Map<String, Object> sourceOffset = new HashMap<>();
                if (headerTs > 0) sourceOffset.put("lastHeaderTs", headerTs);
                sourceOffset.put("lastFetchMs", System.currentTimeMillis());

                out.add(new SourceRecord(
                        sourcePartition, sourceOffset,
                        topic, null,
                        Schema.STRING_SCHEMA, key,
                        sav.schema(), sav.value()
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

    private GenericRecord buildRecord(
            long headerTs,
            GtfsRealtime.FeedEntity ent,
            GtfsRealtime.VehiclePosition v
    ) {

        GtfsRealtime.TripDescriptor trip = v.hasTrip() ? v.getTrip() : null;
        GtfsRealtime.VehicleDescriptor veh = v.hasVehicle() ? v.getVehicle() : null;
        GtfsRealtime.Position pos = v.getPosition();

        Long entityTsSec = v.hasTimestamp() ? v.getTimestamp() : null;

        long tsSec = computeKeyTs(headerTs, entityTsSec);
        long tsMs = tsSec * 1000L;

        ZonedDateTime zoned = Instant.ofEpochMilli(tsMs).atZone(BUSINESS_DAY_ZONE);
        LocalTime localTime = zoned.toLocalTime();

        int hour   = localTime.getHour();
        int minute = localTime.getMinute();
        int second = localTime.getSecond();

        String arrivalTimeLocal = String.format("%02d:%02d:%02d", hour, minute, second);
        String arrivalTimeLocalExtended = String.format("%02d:%02d:%02d", hour + 24, minute, second);

        int arrivalTimeLocalSeconds = hour * 3600 + minute * 60 + second;
        int arrivalTimeLocalExtendedSeconds = arrivalTimeLocalSeconds + 24 * 3600;

        double lat = pos.getLatitude();
        double lon = pos.getLongitude();

        String vehicleId = (veh != null && veh.hasId()) ? veh.getId() : null;
        String tripId = (trip != null && trip.hasTripId()) ? trip.getTripId() : null;

        GenericRecord rec = new GenericData.Record(VehiclePositionSchemas.VEHICLE_POSITION_SCHEMA);
        rec.put("agency", agency);
        rec.put("vehicle_id", vehicleId);
        rec.put("trip_id", tripId);
        rec.put("latitude", lat);
        rec.put("longitude", lon);
        rec.put("ts_ms", tsMs);
        rec.put("time_local", arrivalTimeLocal);
        rec.put("time_local_extended", arrivalTimeLocalExtended);
        rec.put("time_local_seconds", arrivalTimeLocalSeconds);
        rec.put("time_local_extended_seconds", arrivalTimeLocalExtendedSeconds);
        rec.put("local_date", zoned.toLocalDate().toString());
        rec.put("prev_local_date", zoned.toLocalDate().minusDays(1).toString());

        return rec;
    }

    private long computeKeyTs(long headerTs, Long entityTsSec) {
        if (entityTsSec != null && entityTsSec > 0) {
            return entityTsSec;
        }
        if (headerTs > 0) {
            return headerTs;
        }
        return Instant.now().getEpochSecond();
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
