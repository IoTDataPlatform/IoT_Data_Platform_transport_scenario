package iot.data.platform.core;

public record VehicleState(
        double progressM,
        int nextStopIdx,
        long lastTsMillis
) {
    public static VehicleState empty() {
        return new VehicleState(0.0, 0, -1L);
    }
}
