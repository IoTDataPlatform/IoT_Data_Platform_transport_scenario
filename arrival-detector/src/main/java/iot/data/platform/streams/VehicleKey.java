package iot.data.platform.streams;

public record VehicleKey(
        String agency,
        String vehicleId,
        String tripId
) {
}
