package iot.data.platform.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class TripRuntime {
    final List<Types.StopPoint> stops;
    final List<Types.ShapePoint> shape;
    final double[] stopS;
    final long scheduledStartTimeMillis;

    public TripRuntime(
            List<Types.StopPoint> stops,
            List<Types.ShapePoint> rawShape,
            long scheduledStartTimeMillis,
            double densifyStepM
    ) {
        this.stops = new ArrayList<>(Objects.requireNonNull(stops, "stops"));
        this.stops.sort(Comparator.comparingInt(Types.StopPoint::stopSequence));

        List<Types.ShapePoint> baseShape = (rawShape == null || rawShape.isEmpty())
                ? pseudoShapeFromStops(this.stops)
                : rawShape;

        this.shape = PolylineUtils.densify(baseShape, densifyStepM);
        this.stopS = new double[this.stops.size()];

        for (int i = 0; i < this.stops.size(); i++) {
            Types.StopPoint sp = this.stops.get(i);
            this.stopS[i] = PolylineProjection.projectAlongMeters(sp.lat(), sp.lon(), this.shape);
        }

        this.scheduledStartTimeMillis = scheduledStartTimeMillis;
    }

    private static List<Types.ShapePoint> pseudoShapeFromStops(List<Types.StopPoint> stops) {
        List<Types.StopPoint> sorted = new ArrayList<>(stops);
        sorted.sort(Comparator.comparingInt(Types.StopPoint::stopSequence));
        List<Types.ShapePoint> pts = new ArrayList<>(sorted.size());
        for (Types.StopPoint s : sorted) {
            pts.add(new Types.ShapePoint(s.lat(), s.lon()));
        }
        return pts;
    }
}
