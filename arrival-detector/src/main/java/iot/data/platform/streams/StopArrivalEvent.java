package iot.data.platform.streams;

public record StopArrivalEvent(
        String agency,
        String vehicleId,
        String tripId,
        String stopId,
        int stopSequence,
        long arrivalTimeMillis,
        String arrivalTimeLocal,
        String arrivalTimeLocalExtended,
        int arrivalTimeLocalSeconds,
        int arrivalTimeLocalExtendedSeconds
) {
}
