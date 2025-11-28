package iot.data.platform.streams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import iot.data.platform.core.Config;
import iot.data.platform.core.TripRuntimeFactory;
import iot.data.platform.core.Types;
import iot.data.platform.core.VehicleState;
import iot.data.platform.spi.ShapeProvider;
import iot.data.platform.spi.StopProvider;
import iot.data.platform.spi.TripScheduleProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public final class ArrivalDetectorTopology {

    private static final Schema STOP_ARRIVAL_SCHEMA;

    static {
        String schemaString = """
                {
                  "namespace": "iot.data.platform.avro",
                  "type": "record",
                  "name": "StopArrivalEvent",
                  "fields": [
                    { "name": "agency", "type": "string" },
                    { "name": "vehicle_id", "type": "string" },
                    { "name": "trip_id", "type": "string" },
                    { "name": "stop_id", "type": "string" },
                    { "name": "stop_sequence", "type": "int" },
                    { "name": "arrival_time_millis", "type": "long" },
                    { "name": "arrival_time_local", "type": "string" },
                    { "name": "arrival_time_local_extended", "type": "string" },
                    { "name": "arrival_time_local_seconds", "type": "int" },
                    { "name": "arrival_time_local_extended_seconds", "type": "int" },
                    { "name": "arrival_date", "type": "string" },
                    { "name": "arrival_prev_date", "type": "string" }
                  ]
                }
                """;
        STOP_ARRIVAL_SCHEMA = new Schema.Parser().parse(schemaString);
    }

    private ArrivalDetectorTopology() {
    }

    public static Topology build(
            String inputTopic,
            String outputTopic,
            StopProvider stopProvider,
            ShapeProvider shapeProvider,
            TripScheduleProvider scheduleProvider,
            Config cfg,
            String schemaRegistryUrl,
            ZoneId arrivalZoneId
    ) {
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);

        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(serdeConfig, false);

        KStream<byte[], GenericRecord> raw =
                builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), genericAvroSerde));

        KStream<VehicleKey, Types.PositionSample> positions =
                raw.selectKey((oldKey, rec) -> {
                            if (rec == null) return null;

                            String agency = rec.get("agency").toString();

                            Object vehicleIdVal = rec.get("vehicle_id");
                            Object tripIdVal = rec.get("trip_id");

                            String vehicleId = "".equals(vehicleIdVal.toString()) ? null : vehicleIdVal.toString();
                            String tripId = "".equals(tripIdVal.toString()) ? null : tripIdVal.toString();

                            return new VehicleKey(agency, vehicleId, tripId);
                        })
                        .mapValues(rec -> {
                            if (rec == null) return null;

                            Object vehicleIdVal = rec.get("vehicle_id");
                            Object tripIdVal = rec.get("trip_id");

                            String vehicleId = "".equals(vehicleIdVal.toString()) ? null : vehicleIdVal.toString();
                            String tripId = "".equals(tripIdVal.toString()) ? null : tripIdVal.toString();

                            double lat = (double) rec.get("latitude");
                            double lon = (double) rec.get("longitude");
                            long tsMs = (long) rec.get("ts_ms");

                            return new Types.PositionSample(
                                    vehicleId,
                                    tripId,
                                    lat,
                                    lon,
                                    tsMs
                            );
                        });

        KeyValueBytesStoreSupplier storeSupplier =
                Stores.persistentKeyValueStore(ArrivalDetectorProcessor.VEHICLE_STATE_STORE_NAME);

        Serde<VehicleKey> vehicleKeySerde = new JsonSerde<>(VehicleKey.class);
        Serde<VehicleState> vehicleStateSerde = new JsonSerde<>(VehicleState.class);

        StoreBuilder<KeyValueStore<VehicleKey, VehicleState>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, vehicleKeySerde, vehicleStateSerde);

        builder.addStateStore(storeBuilder);

        TripRuntimeFactory tripRuntimeFactory =
                new TripRuntimeFactory(stopProvider, shapeProvider, scheduleProvider, cfg);

        KStream<VehicleKey, StopArrivalEvent> arrivals =
                positions.process(
                        () -> new ArrivalDetectorProcessor(tripRuntimeFactory, cfg, arrivalZoneId),
                        ArrivalDetectorProcessor.VEHICLE_STATE_STORE_NAME
                );

        KStream<VehicleKey, GenericRecord> avroArrivals =
                arrivals.mapValues(event -> {
                    if (event == null) return null;

                    GenericRecord rec = new GenericData.Record(STOP_ARRIVAL_SCHEMA);
                    rec.put("agency", event.agency());
                    rec.put("vehicle_id", event.vehicleId());
                    rec.put("trip_id", event.tripId());
                    rec.put("stop_id", event.stopId());
                    rec.put("stop_sequence", event.stopSequence());
                    rec.put("arrival_time_millis", event.arrivalTimeMillis());
                    rec.put("arrival_time_local", event.arrivalTimeLocal());
                    rec.put("arrival_time_local_extended", event.arrivalTimeLocalExtended());
                    rec.put("arrival_time_local_seconds", event.arrivalTimeLocalSeconds());
                    rec.put("arrival_time_local_extended_seconds", event.arrivalTimeLocalExtendedSeconds());
                    rec.put("arrival_date", event.arrivalDate());
                    rec.put("arrival_prev_date", event.arrivalPrevDate());
                    return rec;
                });

        avroArrivals.to(outputTopic, Produced.with(vehicleKeySerde, genericAvroSerde));

        return builder.build();
    }
}
