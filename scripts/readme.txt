=============================================================================

Назначение
----------
Этот проект поднимает локальный стек Kafka + Kafka Connect + Redis + Kafka UI и запускает два коннектора:
1) GTFS RT Ingestor Source — читает GTFS-RT VehiclePositions (protobuf) из URL и пишет в Kafka топик.
2) Position Cache Redis Sink — читает сообщения из Kafka и пишет текущее состояние каждого ТС в Redis Hash.
(Опционально) Filedump sink — сбрасывает те же сообщения в NDJSON-файл для отладки.

Требования
----------
- Docker и Docker Compose
- make, curl
- Скомпилированные .jar плагинов в каталоге ./connect-plugins (маппится в контейнер Kafka Connect)

Структура
---------
./docker-compose.yml
./connect-plugins/               # сюда кладём jar-ники коннекторов
./configs/
  ├─ gtfs-rt-sl-source.json      # конфиг источника
  ├─ vehpos-redis-sink.json      # конфиг sink-а в Redis
  └─ vehpos-filedump-sink.json   # конфиг sink-а в файл (опционально)
Makefile

Полезные URL
------------
- Kafka Connect REST:  http://localhost:8083
- Kafka UI:            http://localhost:8080
- Kafka (host):        PLAINTEXT 127.0.0.1:9094  (внутри сети: kafka:9092)
- Redis:               127.0.0.1:6379

Быстрый старт
-------------
1) Положите jar-файлы плагинов в ./connect-plugins
2) Проверьте/отредактируйте JSON-конфиги в ./configs
3) Поднимите стек:
   make up
4) Включите коннекторы (по отдельности или все разом):
   make enable-gtfs-rt-ingestor-source
   make enable-position-cache-redis-sink
   # опционально:
   make enable-filedump-sink
   # или одной командой:
   make enable-all
5) Смотрите статусы/логи:
   make connectors
   make status NAME=gtfs-rt-sl-source
   make logs

Остановка / очистка
-------------------
- Выключить коннекторы:
  make disable-all
- Остановить контейнеры:
  make down
- Остановить контейнеры и удалить тома (Kafka данные будут утеряны):
  make clean-volumes

Краткое описание команд Makefile
--------------------------------
Базовые команды Docker Compose:
- make up               — поднять все сервисы в фоне
- make down             — остановить и удалить сервисы
- make clean-volumes    — down + удаление томов
- make restart          — перезапуск (down → up)
- make logs             — стрим логов всех сервисов
- make ps               — статус контейнеров

Утилиты Kafka Connect:
- make connect-health   — ожидать, пока Connect поднимется
- make plugins          — список зарегистрированных плагинов
- make connectors       — список коннекторов
- make status NAME=<connector>  — статус одного коннектора

Управление GTFS RT Ingestor Source:
- make enable-gtfs-rt-ingestor-source
- make disable-gtfs-rt-ingestor-source
- make pause-gtfs-rt-ingestor-source
- make resume-gtfs-rt-ingestor-source
- make restart-gtfs-rt-ingestor-source

Управление Position Cache Redis Sink:
- make enable-position-cache-redis-sink
- make disable-position-cache-redis-sink
- make pause-position-cache-redis-sink
- make resume-position-cache-redis-sink
- make restart-position-cache-redis-sink

(Опционально) Управление FileDump sink:
- make enable-filedump-sink
- make disable-filedump-sink
- make pause-filedump-sink
- make resume-filedump-sink
- make restart-filedump-sink

Групповые команды:
- make enable-all       — включить основной source и Redis sink
- make disable-all      — выключить все (включая filedump)

Переменные окружения/переопределения
------------------------------------
Можно переопределять переменные Makefile при вызове:
- CONNECT_URL — URL Kafka Connect REST (по умолчанию http://localhost:8083)
- CONNECTORS_CONFIGS_DIR — путь к JSON конфигам (по умолчанию ./configs)
- DC — команда docker compose (по умолчанию 'docker compose')

Примеры:
  CONNECT_URL=http://127.0.0.1:8083 make connectors
  CONNECTORS_CONFIGS_DIR=./prod-configs make enable-all

Отладка и советы
----------------
- Если коннектор не создаётся: см. make logs (контейнер kafka-connect), проверьте, что jar плагина виден в /usr/share/confluent-hub-components внутри контейнера.
- Если значения в Redis не появляются: проверьте, что топик, указанный в sink-конфиге, совпадает с топиком из source.
- Большие сообщения: при необходимости увеличьте лимиты клиента у Kafka Connect (PRODUCER_MAX_REQUEST_SIZE и т.д.).
- Конвертеры: в конфигурации коннекторов используется JsonConverter с schemas.enable=true; это совместимо с Struct из Kafka Connect API.