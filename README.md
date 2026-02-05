### Модули
- `ingestor/` - **Kafka Connect Source**: читает GTFS-RT по HTTP, конвертирует в Avro и отправляет в Kafka.
- `file-dump/` - **Kafka Connect Sink**: пишет Kafka сообщения в файл `.ndjson` (для отладки).
- `arrival-detector/` - **Kafka Streams** приложение: детектирует прибытия на остановки и пишет события в Kafka.

### Инфраструктура и запуск
- `scripts/docker-compose.yml` - весь локальный стек (Kafka, Connect, Redis, Postgres, MinIO, Iceberg REST, Trino, Flink, UI).
- `scripts/Makefile` - команды для поднятия окружения и управления коннекторами через Connect REST API.
- `scripts/configs/*.json` - конфиги Kafka Connect коннекторов (source/sinks).
