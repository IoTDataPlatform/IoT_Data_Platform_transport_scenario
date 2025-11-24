CREATE OR REPLACE VIEW stop_times_str AS
SELECT
  trip_id,
  stop_id,
  stop_sequence,
  to_char(arrival_time, 'HH24:MI:SS') AS arrival_time
FROM stop_times;