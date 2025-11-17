package iot.data.platform.backend;

import iot.data.platform.core.Types;
import iot.data.platform.spi.ShapeProvider;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpShapeProvider implements ShapeProvider {
    private final HttpJsonClient client;

    private final Map<String, List<Types.ShapePoint>> cache = new ConcurrentHashMap<>();

    public HttpShapeProvider(String baseUrl) {
        this.client = new HttpJsonClient(baseUrl);
    }

    @Override
    public List<Types.ShapePoint> getShape(String tripId) {
        return cache.computeIfAbsent(tripId, this::loadShapeFromBackend);
    }

    private List<Types.ShapePoint> loadShapeFromBackend(String tripId) {
        HttpJsonClient.ShapeResponse resp =
                client.get("/api/trips/" + tripId + "/shape", HttpJsonClient.ShapeResponse.class);

        if (resp.points == null || resp.points.isEmpty()) {
            return List.of();
        }

        return resp.points.stream()
                .sorted(Comparator.comparingInt(p -> p.sequence))
                .map(p -> new Types.ShapePoint(p.lat, p.lon))
                .toList();
    }
}
