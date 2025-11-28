package iot.data.platform.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class StopArrivalAlgorithm {
    private final Config cfg;

    public StopArrivalAlgorithm(Config cfg) {
        this.cfg = (cfg == null) ? Config.defaults() : cfg;
    }

    public record Result(
            VehicleState newState,
            List<Types.StopArrival> arrivals
    ) {}

    public Result detect(
            Types.PositionSample s,
            TripRuntime trip,
            VehicleState prevState
    ) {
        Objects.requireNonNull(s, "sample");
        Objects.requireNonNull(trip, "trip");
        VehicleState vs = (prevState == null) ? VehicleState.empty() : prevState;
        if (trip.stops.isEmpty()) {
            double sNow = PolylineProjection.projectAlongMeters(s.lat(), s.lon(), trip.shape);
            VehicleState newState = new VehicleState(
                    Math.max(vs.progressM(), sNow),
                    vs.nextStopIdx(),
                    s.tsMillis()
            );
            return new Result(newState, List.of());
        }
        double sNow = PolylineProjection.projectAlongMeters(s.lat(), s.lon(), trip.shape);
        long t2 = s.tsMillis();

        double sPrev;
        long t1;
        boolean hasPrevSample = vs.lastTsMillis() >= 0;
        long scheduledStart = trip.scheduledStartTimeMillis;

        boolean canUseScheduleAnchor =
                !hasPrevSample &&
                        scheduledStart >= 0 &&
                        scheduledStart <= t2 &&
                        (t2 - scheduledStart) <= cfg.maxScheduleAnchorGapMillis();

        if (hasPrevSample) {
            sPrev = vs.progressM();
            t1 = vs.lastTsMillis();
        } else if (canUseScheduleAnchor) {
            sPrev = 0.0;
            t1 = scheduledStart;
        } else {
            sPrev = sNow;
            t1 = t2;
        }

        double tol = cfg.toleranceM();
        double sMin = Math.min(sPrev, sNow);
        double sMax = Math.max(sPrev, sNow);

        List<Types.StopArrival> arrivals = new ArrayList<>();
        int k = vs.nextStopIdx();

        if (k >= trip.stops.size()) {
            double newProgress = Math.max(vs.progressM(), sNow);
            VehicleState newState = new VehicleState(newProgress, k, t2);
            return new Result(newState, List.of());
        }

        while (k < trip.stops.size()) {
            double lp = trip.stopS[k];

            if (lp + tol < sMin) {
                k++;
                continue;
            }

            if (lp <= sMax + tol) {
                double ds = sNow - sPrev;

                long arrivalTime;
                if (Math.abs(ds) < 1e-6) {
                    arrivalTime = t2;
                } else {
                    double frac = (lp - sPrev) / ds;
                    if (frac < 0.0) frac = 0.0;
                    if (frac > 1.0) frac = 1.0;
                    arrivalTime = t1 + Math.round(frac * (t2 - t1));
                }

                Types.StopPoint stop = trip.stops.get(k);
                arrivals.add(new Types.StopArrival(
                        s.vehicleId(),
                        s.tripId(),
                        stop.stopId(),
                        stop.stopSequence(),
                        arrivalTime
                ));

                k++;
            } else {
                break;
            }
        }

        int nextIdx = Math.min(k, trip.stops.size());
        double newProgress = Math.max(vs.progressM(), sNow);
        VehicleState newState = new VehicleState(newProgress, nextIdx, t2);

        return new Result(newState, arrivals);
    }
}
