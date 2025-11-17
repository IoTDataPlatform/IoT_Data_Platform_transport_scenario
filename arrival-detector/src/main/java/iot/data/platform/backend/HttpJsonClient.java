package iot.data.platform.backend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

class HttpJsonClient {
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;

    HttpJsonClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
    }

    <T> T get(String path, Class<T> type) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .GET()
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException("HTTP " + resp.statusCode() + " for " + path);
            }
            return mapper.readValue(resp.body(), type);
        } catch (Exception e) {
            throw new RuntimeException("HTTP GET failed: " + path, e);
        }
    }

    ObjectMapper mapper() {
        return mapper;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ShapeResponse {
        public String tripId;
        public String routeId;
        public String shapeId;
        public java.util.List<Point> points;

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Point {
            public double lat;
            public double lon;
            public int sequence;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class StopsResponse {
        public String tripId;
        public String routeId;
        public java.util.List<StopDto> stops;

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class StopDto {
            public String stopId;
            public String stopName;
            public double lat;
            public double lon;
            public int sequence;
            public String arrivalTime;
            public String departureTime;
        }
    }
}
