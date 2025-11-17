package iot.data.platform.core;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;

final class Geo {
    private Geo() {}

    static double distanceM(double lat1, double lon1, double lat2, double lon2) {
        GeodesicData inv = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2);
        return inv.s12;
    }
}
