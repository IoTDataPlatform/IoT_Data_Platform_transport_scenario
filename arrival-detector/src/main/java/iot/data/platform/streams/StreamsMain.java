package iot.data.platform.streams;

import iot.data.platform.backend.HttpShapeProvider;
import iot.data.platform.backend.HttpStopProvider;
import iot.data.platform.core.Config;
import iot.data.platform.spi.ShapeProvider;
import iot.data.platform.spi.StopProvider;
import iot.data.platform.spi.TripScheduleProvider;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.ZoneId;
import java.util.Properties;

public class StreamsMain {
    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
        String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String backendBaseUrl = env("GTFS_BACKEND_URL", "http://localhost:8082");
        String inputTopic = env("INPUT_TOPIC", "gtfs.vehicle.positions.sl");
        String outputTopic = env("OUTPUT_TOPIC", "stop_arrivals");
        String arrivalTimeZoneId = env("ARRIVAL_TIME_ZONE", "UTC+1");
        ZoneId arrivalZoneId = ZoneId.of(arrivalTimeZoneId);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "arrival-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StopProvider stopProvider = new HttpStopProvider(backendBaseUrl);
        ShapeProvider shapeProvider = new HttpShapeProvider(backendBaseUrl);
        TripScheduleProvider scheduleProvider = tripId -> null;

        Config cfg = Config.defaults();

        Topology topology = ArrivalDetectorTopology.build(
                inputTopic,
                outputTopic,
                stopProvider,
                shapeProvider,
                scheduleProvider,
                cfg,
                schemaRegistryUrl,
                arrivalZoneId
        );

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}