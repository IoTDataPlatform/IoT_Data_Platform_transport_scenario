package iot.data.platform.ingestor.fetcher;

import java.time.Duration;
import java.util.Map;

public final class UrlProfile {
    public enum Decompression {AUTO, GZIP, DEFLATE, NONE, TRY_SMART}

    public final String accept;
    public final String acceptEncoding;
    public final Map<String, String> headers;
    public final boolean conditionalGet;
    public final Decompression decompression;
    public final Duration timeout;
    public final String userAgent;

    public UrlProfile(
            String accept,
            String acceptEnc,
            Map<String, String> headers,
            boolean conditionalGet,
            Decompression dec,
            Duration timeout,
            String ua
    ) {
        this.accept = accept;
        this.acceptEncoding = acceptEnc;
        this.headers = headers;
        this.conditionalGet = conditionalGet;
        this.decompression = dec;
        this.timeout = timeout;
        this.userAgent = ua;
    }

    public static UrlProfile defaultProfile() {
        return new UrlProfile(
                "application/x-protobuf, application/octet-stream",
                "gzip, deflate",
                Map.of(),
                true,
                Decompression.AUTO,
                Duration.ofSeconds(20),
                "gtfsrt-fetcher/1.0"
        );
    }

    public static UrlProfile trafiklab() {
        return new UrlProfile(
                "application/x-protobuf, application/octet-stream",
                "gzip, deflate",
                Map.of("Accept-Language", "en"),
                true,
                Decompression.AUTO,
                Duration.ofSeconds(15),
                "gtfsrt-fetcher/1.0 (trafiklab)"
        );
    }
}