package iot.data.platform.spi;

import iot.data.platform.core.Types;

public interface TripScheduleProvider {
    Types.TripSchedule getSchedule(String tripId);
}
