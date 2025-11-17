package iot.data.platform.demo;

import iot.data.platform.core.*;
import iot.data.platform.spi.ShapeProvider;
import iot.data.platform.spi.StopProvider;

import java.util.List;


public class DemoMain {
    public static void main(String[] args) {
        StopProvider stops = tripId -> List.of(
                new Types.StopPoint("S1", 55.0000, 37.0000, 1),
                new Types.StopPoint("S2", 55.0006, 37.0008, 2),
                new Types.StopPoint("S3", 55.0012, 37.0016, 3),
                new Types.StopPoint("S4", 55.0018, 37.0024, 4),
                new Types.StopPoint("S5", 55.0024, 37.0032, 5)
        );

        ShapeProvider shapes = id -> List.of();

        String tripId = "T1";
        TripRuntime trip = new TripRuntime(
                stops.getStops(tripId),
                shapes.getShape(tripId),
                0,
                Config.defaults().densifyStepM()
        );

        StopArrivalAlgorithm alg = new StopArrivalAlgorithm(Config.defaults());
        VehicleState state = null;

        for (Types.PositionSample s : demoSamples(tripId)) {
            StopArrivalAlgorithm.Result r = alg.detect(s, trip, state);
            state = r.newState();

            for (Types.StopArrival a : r.arrivals()) {
                System.out.printf("ARRIVAL: vehicle=%s trip=%s stop=%s seq=%d at=%d%n",
                        a.vehicleId(), a.tripId(), a.stopId(), a.stopSequence(), a.arrivalTimeMillis());
            }
        }
    }

    private static List<Types.PositionSample> demoSamples(String trip) {
        return List.of(
                new Types.PositionSample("V1", trip, 55.00002, 37.00001, 0),
                new Types.PositionSample("V1", trip, 55.00040, 37.00055, 8_000),
                new Types.PositionSample("V1", trip, 55.00145, 37.00190, 15_000),
                new Types.PositionSample("V1", trip, 55.00188, 37.00245, 20_000),
                new Types.PositionSample("V1", trip, 55.00295, 37.00395, 28_000),
                new Types.PositionSample("V1", trip, 55.00340, 37.00455, 35_000),
                new Types.PositionSample("V1", trip, 55.00410, 37.00545, 42_000),
                new Types.PositionSample("V1", trip, 55.00535, 37.00705, 55_000),
                new Types.PositionSample("V1", trip, 55.00580, 37.00770, 65_000)
        );
    }
}
