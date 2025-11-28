package iot.data.platform.backend;

import iot.data.platform.core.Types;
import iot.data.platform.spi.StopProvider;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpStopProvider implements StopProvider {
    private final HttpJsonClient client;

    private final Map<String, List<Types.StopPoint>> cache = new ConcurrentHashMap<>();

    public HttpStopProvider(String baseUrl) {
        this.client = new HttpJsonClient(baseUrl);
    }

    @Override
    public List<Types.StopPoint> getStops(String tripId) {
        return cache.computeIfAbsent(tripId, this::loadStopsFromBackend);
    }

    private List<Types.StopPoint> loadStopsFromBackend(String tripId) {
        HttpJsonClient.StopsResponse resp =
                client.get("/api/trips/" + tripId + "/stops", HttpJsonClient.StopsResponse.class);

        if (resp.stops == null || resp.stops.isEmpty()) {
            return List.of();
        }

        return resp.stops.stream()
                .sorted(Comparator.comparingInt(s -> s.sequence))
                .map(s -> new Types.StopPoint(
                        s.stopId,
                        s.lat,
                        s.lon,
                        s.sequence
                ))
                .toList();
    }
}
