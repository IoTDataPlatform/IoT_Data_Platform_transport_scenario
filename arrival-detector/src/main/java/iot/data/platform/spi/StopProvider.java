package iot.data.platform.spi;

import iot.data.platform.core.Types;

import java.util.List;

public interface StopProvider {
    List<Types.StopPoint> getStops(String tripId);
}
