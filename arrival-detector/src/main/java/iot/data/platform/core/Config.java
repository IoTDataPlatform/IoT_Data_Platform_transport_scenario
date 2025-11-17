package iot.data.platform.core;

public record Config(
        double densifyStepM,
        double toleranceM,
        long maxScheduleAnchorGapMillis
) {
    public Config {
        if (densifyStepM <= 0) throw new IllegalArgumentException("densifyStepM must be > 0");
        if (toleranceM < 0) throw new IllegalArgumentException("toleranceM must be >= 0");
        if (maxScheduleAnchorGapMillis <= 0) throw new IllegalArgumentException("maxScheduleAnchorGapMillis must be > 0");
    }

    public static Config defaults() {
        return new Config(10.0, 0.0, 30 * 60 * 1000L);
    }
}
