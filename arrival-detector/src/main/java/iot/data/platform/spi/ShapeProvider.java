package iot.data.platform.spi;

import iot.data.platform.core.Types;

import java.util.List;

public interface ShapeProvider {
    List<Types.ShapePoint> getShape(String tripId);
}
