package iot.data.platform.core;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;

import java.util.List;

final class PolylineProjection {
    private PolylineProjection() {}

    static double projectAlongMeters(double plat, double plon, List<Types.ShapePoint> shape) {
        if (shape == null || shape.size() <= 1) return 0.0;

        double bestCum = 0.0;
        double bestDist = Double.POSITIVE_INFINITY;
        double acc = 0.0;

        for (int i = 0; i < shape.size() - 1; i++) {
            Types.ShapePoint A = shape.get(i);
            Types.ShapePoint B = shape.get(i + 1);

            GeodesicData invAB = Geodesic.WGS84.Inverse(A.lat(), A.lon(), B.lat(), B.lon());
            double dAB = invAB.s12;

            if (dAB == 0.0) continue;

            GeodesicData invAP = Geodesic.WGS84.Inverse(A.lat(), A.lon(), plat, plon);
            double dAP = invAP.s12;

            double t = Math.max(0.0, Math.min(1.0, dAP / dAB));
            double tM = dAB * t;

            GeodesicData proj = Geodesic.WGS84.Direct(A.lat(), A.lon(), invAB.azi1, tM);
            double dProjP = Geodesic.WGS84.Inverse(proj.lat2, proj.lon2, plat, plon).s12;

            double distToSeg = dProjP;
            if (Double.isNaN(distToSeg)) {
                double dBP = Geodesic.WGS84.Inverse(B.lat(), B.lon(), plat, plon).s12;
                distToSeg = dBP;
            }

            if (distToSeg < bestDist) {
                bestDist = distToSeg;
                bestCum = acc + tM;
            }

            acc += dAB;
        }
        return bestCum;
    }
}
