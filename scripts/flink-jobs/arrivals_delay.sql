SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 1. Источник: stop_arrivals из Kafka
CREATE TABLE stop_arrivals (
  agency                              STRING,
  vehicle_id                          STRING,
  trip_id                             STRING,
  stop_id                             STRING,
  stop_sequence                       INT,

  arrival_time_millis                 BIGINT,
  arrival_time_local                  STRING,
  arrival_time_local_extended         STRING,
  arrival_time_local_seconds          INT,
  arrival_time_local_extended_seconds INT,
  arrival_date                        STRING,
  arrival_prev_date                   STRING,

  proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'stop_arrivals',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-stop-arrivals-g1',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://kafka-schema-registry:8081'
);

-- 2. trips: trip_id -> service_id (+ route_id)
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

-- 3. calendar_dates: service_id + дата (DATE)
CREATE TEMPORARY TABLE calendar_dates (
  service_id   STRING NOT NULL,
  `date`       DATE   NOT NULL,
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

-- 4. stop_times: плановые времена прибытия
CREATE TEMPORARY TABLE stop_times (
  trip_id       STRING NOT NULL,
  stop_id       STRING NOT NULL,
  stop_sequence INT    NOT NULL,
  arrival_time  STRING NOT NULL,  -- HH:MM:SS
  PRIMARY KEY (trip_id, stop_sequence) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gtfs',
  'table-name' = 'stop_times_str',
  'username' = 'app',
  'password' = 'app',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '10 min',
  'driver' = 'org.postgresql.Driver'
);

-- 5. Синк: подробные прибытия с разницей
CREATE TABLE arrivals_delay (
  stop_id              STRING,
  trip_id              STRING,
  route_id             STRING,
  vehicle_id           STRING,

  chosen_date          STRING,
  chosen_time          STRING,
  actual_seconds       INT,

  scheduled_time       STRING,
  scheduled_seconds    INT,

  delay_seconds        INT,
  delay_type           STRING,  -- 'LATE' / 'EARLY' / 'ON_TIME'

  arrival_time_millis  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'arrivals_delay',
  'properties.bootstrap.servers' = 'kafka:9092',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://kafka-schema-registry:8081'
);

-- 6. Основной INSERT
INSERT INTO arrivals_delay
SELECT
  stop_id,
  trip_id,
  route_id,
  vehicle_id,

  chosen_date,
  chosen_time,
  actual_seconds,

  scheduled_time,
  scheduled_seconds,

  actual_seconds - scheduled_seconds AS delay_seconds,
  CASE
    WHEN actual_seconds - scheduled_seconds > 0 THEN 'LATE'
    WHEN actual_seconds - scheduled_seconds < 0 THEN 'EARLY'
    ELSE 'ON_TIME'
  END AS delay_type,

  arrival_time_millis
FROM (
  SELECT
    sa.stop_id,
    sa.trip_id,
    t.route_id,
    sa.vehicle_id,

    -- выбранная дата
    CASE
      WHEN cd.`date` = TO_DATE(sa.arrival_date) THEN sa.arrival_date
      WHEN cd.`date` = TO_DATE(sa.arrival_prev_date) THEN sa.arrival_prev_date
      ELSE NULL
    END AS chosen_date,

    -- выбранное "человеческое" время
    CASE
      WHEN cd.`date` = TO_DATE(sa.arrival_date) THEN sa.arrival_time_local
      WHEN cd.`date` = TO_DATE(sa.arrival_prev_date) THEN sa.arrival_time_local_extended
      ELSE NULL
    END AS chosen_time,

    -- выбранное время в секундах
    CASE
      WHEN cd.`date` = TO_DATE(sa.arrival_date) THEN sa.arrival_time_local_seconds
      WHEN cd.`date` = TO_DATE(sa.arrival_prev_date) THEN sa.arrival_time_local_extended_seconds
      ELSE NULL
    END AS actual_seconds,

    -- плановое время прибытия
    st.arrival_time AS scheduled_time,

    -- плановые секунды от полуночи
    (
      CAST(SUBSTRING(st.arrival_time FROM 1 FOR 2) AS INT) * 3600 +
      CAST(SUBSTRING(st.arrival_time FROM 4 FOR 2) AS INT) * 60  +
      CAST(SUBSTRING(st.arrival_time FROM 7 FOR 2) AS INT)
    ) AS scheduled_seconds,

    sa.arrival_time_millis
  FROM stop_arrivals AS sa
  JOIN trips FOR SYSTEM_TIME AS OF sa.proctime AS t
    ON sa.trip_id = t.trip_id
  LEFT JOIN calendar_dates FOR SYSTEM_TIME AS OF sa.proctime AS cd
    ON t.service_id = cd.service_id
   AND (cd.`date` = TO_DATE(sa.arrival_date)
        OR cd.`date` = TO_DATE(sa.arrival_prev_date))
  LEFT JOIN stop_times FOR SYSTEM_TIME AS OF sa.proctime AS st
    ON sa.trip_id = st.trip_id
   AND sa.stop_id = st.stop_id
) s
WHERE actual_seconds IS NOT NULL
  AND scheduled_time IS NOT NULL;
