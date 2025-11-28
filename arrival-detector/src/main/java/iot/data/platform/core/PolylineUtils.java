package iot.data.platform.core;

import java.util.ArrayList;
import java.util.List;

final class PolylineUtils {
    private PolylineUtils() {}

    static List<Types.ShapePoint> densify(List<Types.ShapePoint> src, double stepM) {
        if (src == null || src.size() <= 1) {
            return (src == null) ? List.of() : List.copyOf(src);
        }
        if (stepM <= 0) return List.copyOf(src);

        List<Types.ShapePoint> out = new ArrayList<>();
        out.add(src.getFirst());
        for (int i = 0; i < src.size() - 1; i++) {
            Types.ShapePoint a = src.get(i);
            Types.ShapePoint b = src.get(i + 1);

            double seg = Geo.distanceM(a.lat(), a.lon(), b.lat(), b.lon());
            if (seg <= stepM) {
                out.add(b);
                continue;
            }

            int n = (int) Math.floor(seg / stepM);
            for (int k = 1; k <= n; k++) {
                double t = (k * stepM) / seg;
                if (t >= 1.0) break;
                double lat = a.lat() + (b.lat() - a.lat()) * t;
                double lon = a.lon() + (b.lon() - a.lon()) * t;
                out.add(new Types.ShapePoint(lat, lon));
            }
            out.add(b);
        }
        return out;
    }
}
