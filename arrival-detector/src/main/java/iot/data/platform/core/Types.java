package iot.data.platform.core;

import java.util.List;

public final class Types {
    private Types() {}

    public record PositionSample(
            String vehicleId,
            String tripId,
            double lat,
            double lon,
            long tsMillis
    ) {}

    public record StopPoint(
            String stopId,
            double lat,
            double lon,
            int stopSequence
    ) {}

    public record ShapePoint(
            double lat,
            double lon
    ) {}

    public record StopArrival(
            String vehicleId,
            String tripId,
            String stopId,
            int stopSequence,
            long arrivalTimeMillis
    ) {}

    public record TripSchedule(
            long scheduledStartTimeMillis
    ) {}

    public interface StopArrivalListener {
        void onStopArrival(StopArrival arrival);
    }
}
