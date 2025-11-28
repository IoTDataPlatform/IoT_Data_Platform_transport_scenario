package iot.data.platform.streams;

import iot.data.platform.core.*;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class ArrivalDetectorProcessor extends ContextualProcessor<VehicleKey, Types.PositionSample, VehicleKey, StopArrivalEvent> {
    public static final String VEHICLE_STATE_STORE_NAME = "vehicle-state-store";

    private final TripRuntimeFactory tripRuntimeFactory;
    private final StopArrivalAlgorithm algorithm;
    private final ZoneId arrivalZoneId;

    private KeyValueStore<VehicleKey, VehicleState> stateStore;

    public ArrivalDetectorProcessor(TripRuntimeFactory tripRuntimeFactory, Config cfg, ZoneId arrivalZoneId) {
        this.tripRuntimeFactory = Objects.requireNonNull(tripRuntimeFactory, "tripRuntimeFactory");
        this.algorithm = new StopArrivalAlgorithm(cfg);
        this.arrivalZoneId = Objects.requireNonNull(arrivalZoneId, "arrivalZoneId");
    }

    @Override
    public void init(ProcessorContext<VehicleKey, StopArrivalEvent> context) {
        super.init(context);
        this.stateStore = context.getStateStore(VEHICLE_STATE_STORE_NAME);
    }

    @Override
    public void process(Record<VehicleKey, Types.PositionSample> record) {
        if (record == null || record.key() == null || record.value() == null) {
            return;
        }

        VehicleKey key = record.key();
        Types.PositionSample sample = record.value();

        if (sample == null || sample.tripId() == null) {
            return;
        }

        TripRuntime trip = tripRuntimeFactory.get(sample.tripId());
        VehicleState prevState = stateStore.get(key);

        StopArrivalAlgorithm.Result result =
                algorithm.detect(sample, trip, prevState);

        stateStore.put(key, result.newState());

        for (Types.StopArrival a : result.arrivals()) {
            long arrivalTsMillis = a.arrivalTimeMillis();

            ZonedDateTime zoned = ZonedDateTime.ofInstant(
                    Instant.ofEpochMilli(arrivalTsMillis),
                    arrivalZoneId
            );
            LocalTime localTime = zoned.toLocalTime();

            int hour   = localTime.getHour();
            int minute = localTime.getMinute();
            int second = localTime.getSecond();

            String arrivalTimeLocal = String.format("%02d:%02d:%02d", hour, minute, second);
            String arrivalTimeLocalExtended = String.format("%02d:%02d:%02d", hour + 24, minute, second);

            int arrivalTimeLocalSeconds = hour * 3600 + minute * 60 + second;
            int arrivalTimeLocalExtendedSeconds = arrivalTimeLocalSeconds + 24 * 3600;

            StopArrivalEvent event = new StopArrivalEvent(
                    key.agency(),
                    a.vehicleId(),
                    a.tripId(),
                    a.stopId(),
                    a.stopSequence(),
                    arrivalTsMillis,
                    arrivalTimeLocal,
                    arrivalTimeLocalExtended,
                    arrivalTimeLocalSeconds,
                    arrivalTimeLocalExtendedSeconds,
                    zoned.toLocalDate().toString(),
                    zoned.toLocalDate().minusDays(1).toString()
            );
            context().forward(record.withValue(event));
        }
    }
}
