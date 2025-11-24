SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Источник: сырые позиции из Kafka с полным набором временных полей
CREATE TABLE positions (
  agency                         STRING,
  vehicle_id                     STRING,
  trip_id                        STRING,
  latitude                       DOUBLE,
  longitude                      DOUBLE,

  ts_ms                          BIGINT,
  time_local                     STRING,
  time_local_extended            STRING,
  time_local_seconds             INT,
  time_local_extended_seconds    INT,
  local_date                     STRING,
  prev_local_date                STRING,

  proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'gtfs.vehicle.positions.sl',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-g1',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://kafka-schema-registry:8081'
);

-- Луккап-таблица из Postgres с trip_id -> route_id
CREATE TEMPORARY TABLE trips (
  trip_id       STRING NOT NULL,
  route_id      STRING NOT NULL,
  service_id    STRING NOT NULL,
  PRIMARY KEY (trip_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gtfs',
  'table-name' = 'trips',
  'username' = 'app',
  'password' = 'app',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '10 min',
  'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE calendar_dates (
  service_id     STRING NOT NULL,
  `date`         DATE  NOT NULL,
  PRIMARY KEY (service_id, `date`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gtfs',
  'table-name' = 'calendar_dates',
  'username' = 'app',
  'password' = 'app',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '10 min',
  'driver' = 'org.postgresql.Driver'
);


-- Приёмник: обогащённые позиции с route_id и всем временем
CREATE TABLE positions_detailed (
  agency                         STRING,
  vehicle_id                     STRING,
  trip_id                        STRING,
  latitude                       DOUBLE,
  longitude                      DOUBLE,
  route_id                       STRING,

  chosen_date                    STRING,
  chosen_time                    STRING,
  chosen_seconds                 INT,

  ts_ms                          BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'positions_detailed',
  'properties.bootstrap.servers' = 'kafka:9092',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://kafka-schema-registry:8081'
);

-- Основной стриминговый INSERT: обогащаем позицию route_id и прокидываем всё время как есть
INSERT INTO positions_detailed
SELECT
  p.agency,
  p.vehicle_id,
  p.trip_id,
  p.latitude,
  p.longitude,
  t.route_id,

  CASE
    WHEN cd.`date` = TO_DATE(p.local_date) THEN p.local_date
    WHEN cd.`date` = TO_DATE(p.prev_local_date) THEN p.prev_local_date
    ELSE NULL
  END AS chosen_date,

  CASE
    WHEN cd.`date` = TO_DATE(p.local_date) THEN p.time_local
    WHEN cd.`date` = TO_DATE(p.prev_local_date) THEN p.time_local_extended
    ELSE NULL
  END AS chosen_time,

  CASE
    WHEN cd.`date` = TO_DATE(p.local_date) THEN p.time_local_seconds
    WHEN cd.`date` = TO_DATE(p.prev_local_date) THEN p.time_local_extended_seconds
    ELSE NULL
  END AS chosen_seconds,

  p.ts_ms
FROM positions AS p
JOIN trips FOR SYSTEM_TIME AS OF p.proctime AS t
  ON p.trip_id = t.trip_id
LEFT JOIN calendar_dates FOR SYSTEM_TIME AS OF p.proctime AS cd
  ON t.service_id = cd.service_id
 AND (cd.`date` = TO_DATE(p.local_date) OR cd.`date` = TO_DATE(p.prev_local_date));