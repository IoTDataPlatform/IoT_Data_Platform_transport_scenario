package iot.data.platform.core;

import iot.data.platform.spi.ShapeProvider;
import iot.data.platform.spi.StopProvider;
import iot.data.platform.spi.TripScheduleProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TripRuntimeFactory {
    private final StopProvider stopProvider;
    private final ShapeProvider shapeProvider;
    private final TripScheduleProvider scheduleProvider;
    private final Config cfg;

    private final Map<String, TripRuntime> cache = new ConcurrentHashMap<>();

    public TripRuntimeFactory(
            StopProvider stopProvider,
            ShapeProvider shapeProvider,
            TripScheduleProvider scheduleProvider,
            Config cfg
    ) {
        this.stopProvider = stopProvider;
        this.shapeProvider = shapeProvider;
        this.scheduleProvider = scheduleProvider;
        this.cfg = (cfg == null) ? Config.defaults() : cfg;
    }

    public TripRuntime get(String tripId) {
        return cache.computeIfAbsent(tripId, this::build);
    }

    private TripRuntime build(String tripId) {
        List<Types.StopPoint> stops = stopProvider.getStops(tripId);
        List<Types.ShapePoint> shape = (shapeProvider == null) ? List.of() : shapeProvider.getShape(tripId);
        Types.TripSchedule sched = (scheduleProvider == null) ? null : scheduleProvider.getSchedule(tripId);

        long startMs = (sched == null) ? -1L : sched.scheduledStartTimeMillis();
        return new TripRuntime(
                stops,
                shape,
                startMs,
                cfg.densifyStepM()
        );
    }
}
