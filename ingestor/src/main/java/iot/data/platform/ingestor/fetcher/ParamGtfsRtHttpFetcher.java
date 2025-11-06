package iot.data.platform.ingestor.fetcher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class ParamGtfsRtHttpFetcher implements GtfsRtHttpFetcher {
    private final HttpClient client;
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public ParamGtfsRtHttpFetcher() {
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public byte[] getBytes(String url, UrlProfile urlProfile) throws IOException, InterruptedException {
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .timeout(urlProfile.timeout)
                .header("User-Agent", urlProfile.userAgent);

        if (urlProfile.accept != null && !urlProfile.accept.isBlank()) rb.header("Accept", urlProfile.accept);
        if (urlProfile.acceptEncoding != null && !urlProfile.acceptEncoding.isBlank()) rb.header("Accept-Encoding", urlProfile.acceptEncoding);
        for (Map.Entry<String, String> h : urlProfile.headers.entrySet()) rb.header(h.getKey(), h.getValue());

        if (urlProfile.conditionalGet) {
            CacheEntry ce = cache.get(url);
            if (ce != null) {
                if (ce.etag != null) rb.header("If-None-Match", ce.etag);
                if (ce.lastModified != null) rb.header("If-Modified-Since", ce.lastModified);
            }
        }

        HttpResponse<byte[]> resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofByteArray());
        int sc = resp.statusCode();
        if (sc == 304) return null;
        if (sc != 200) throw new IOException("HTTP " + sc + " fetching " + url);

        if (urlProfile.conditionalGet) {
            String etag = first(resp, "ETag");
            String lm = first(resp, "Last-Modified");
            cache.put(url, new CacheEntry(etag, lm));
        }

        byte[] body = resp.body();
        return decode(body, firstLower(resp), urlProfile.decompression);
    }

    private static String first(HttpResponse<?> r, String name) {
        return r.headers().firstValue(name).orElse(null);
    }

    private static String firstLower(HttpResponse<?> r) {
        String v = first(r, "Content-Encoding");
        return v == null ? "" : v.trim().toLowerCase();
    }

    private static byte[] decode(byte[] body, String contentEnc, UrlProfile.Decompression mode) throws IOException {
        switch (mode) {
            case NONE:
                return body;
            case GZIP:
                return gunzip(body);
            case DEFLATE:
                try {
                    return inflate(body, false);
                } catch (IOException e) {
                    return inflate(body, true);
                }
            case TRY_SMART:
                if (body.length >= 2 && (body[0] == 0x1F && (body[1] & 0xFF) == 0x8B)) return gunzip(body);
                if (body.length >= 2 && (body[0] == 0x78)) {
                    try {
                        return inflate(body, false);
                    } catch (IOException e) {
                        return inflate(body, true);
                    }
                }
                return body;
            case AUTO:
            default:
                if (contentEnc == null || contentEnc.isEmpty() || "identity".equals(contentEnc)) return body;
                if ("gzip".equals(contentEnc) || "x-gzip".equals(contentEnc)) return gunzip(body);
                if ("deflate".equals(contentEnc)) {
                    try {
                        return inflate(body, false);
                    } catch (IOException e) {
                        return inflate(body, true);
                    }
                }
                return body;
        }
    }

    private static byte[] gunzip(byte[] data) throws IOException {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(data));
             ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(1024, data.length))) {
            gis.transferTo(out);
            return out.toByteArray();
        }
    }

    private static byte[] inflate(byte[] data, boolean raw) throws IOException {
        Inflater inf = new Inflater(raw);
        try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data), inf);
             ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(1024, data.length))) {
            iis.transferTo(out);
            return out.toByteArray();
        } finally {
            inf.end();
        }
    }

    private record CacheEntry(String etag, String lastModified) {
    }
}