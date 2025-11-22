package iot.data.platform.avro;

import org.apache.avro.Schema;

public final class VehiclePositionSchemas {

    private VehiclePositionSchemas() {}

    private static final String VEHICLE_POSITION_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "VehiclePosition",
              "namespace": "iot.data.platform.avro",
              "fields": [
                { "name": "agency",       "type": "string" },
                { "name": "vehicle_id",   "type": "string" },
                { "name": "trip_id",      "type": "string" },
                { "name": "latitude",     "type": "double" },
                { "name": "longitude",    "type": "double" },
                { "name": "ts_ms",        "type": "long" },
                { "name": "time_local", "type": "string" },
                { "name": "time_local_extended", "type": "string" },
                { "name": "time_local_seconds", "type": "int" },
                { "name": "time_local_extended_seconds", "type": "int" },
                { "name": "local_date", "type": "string" },
                { "name": "prev_local_date", "type": "string" }
              ]
            }
            """;

    public static final Schema VEHICLE_POSITION_SCHEMA = new Schema.Parser().parse(VEHICLE_POSITION_SCHEMA_JSON);
}
