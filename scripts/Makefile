SHELL := /bin/bash

DC ?= docker compose
CONNECT_URL ?= http://localhost:8083
CONNECTORS_CONFIGS_DIR ?= ./configs

INGESTOR_NAME        ?= gtfs-rt-sl-source
POSITION_CACHE_NAME  ?= vehpos-redis-sink
FILE_SINK_NAME       ?= vehpos-filedump-sink

INGESTOR_JSON        := $(CONNECTORS_CONFIGS_DIR)/$(INGESTOR_NAME).json
POSITION_CACHE_JSON  := $(CONNECTORS_CONFIGS_DIR)/$(POSITION_CACHE_NAME).json
FILE_SINK_JSON       := $(CONNECTORS_CONFIGS_DIR)/$(FILE_SINK_NAME).json

define create_connector
	curl -fsS -o /dev/null -w "%{http_code}" -X POST \
	  -H 'Content-Type: application/json' \
	  --data-binary @$(1) \
	  $(CONNECT_URL)/connectors
endef

define delete_connector
	curl -fsS -o /dev/null -w "%{http_code}" -X DELETE \
	  $(CONNECT_URL)/connectors/$(1)
endef

define pause_connector
	curl -fsS -o /dev/null -w "%{http_code}" -X PUT \
	  $(CONNECT_URL)/connectors/$(1)/pause
endef

define resume_connector
	curl -fsS -o /dev/null -w "%{http_code}" -X PUT \
	  $(CONNECT_URL)/connectors/$(1)/resume
endef

define restart_connector
	curl -fsS -o /dev/null -w "%{http_code}" -X POST \
	  $(CONNECT_URL)/connectors/$(1)/restart
endef

.PHONY: up down clean-volumes restart logs ps
up:
	$(DC) up -d

down:
	$(DC) down

clean-volumes:
	$(DC) down -v

restart: down up

logs:
	$(DC) logs -f

ps:
	$(DC) ps

.PHONY: connect-health plugins connectors status
connect-health:
	@bash -c 'for i in {1..60}; do \
	  curl -fsS $(CONNECT_URL)/connectors >/dev/null && echo "Connect is up" && exit 0; \
	  echo "Waiting for Connect... $$i"; sleep 2; done; \
	  echo "Connect is not responding at $(CONNECT_URL)"; exit 1'

plugins:
	@curl -fsS $(CONNECT_URL)/connector-plugins | jq || true

connectors:
	@curl -fsS $(CONNECT_URL)/connectors | jq || true

status:
ifndef NAME
	$(error Use: make status NAME=<connector-name>)
endif
	@curl -fsS $(CONNECT_URL)/connectors/$(NAME)/status || true

.PHONY: enable-gtfs-rt-ingestor-source disable-gtfs-rt-ingestor-source pause-gtfs-rt-ingestor-source resume-gtfs-rt-ingestor-source restart-gtfs-rt-ingestor-source
enable-gtfs-rt-ingestor-source: connect-health
	@code="$$( $(call create_connector,$(INGESTOR_JSON)) )"; \
	if [ "$$code" = "201" ] || [ "$$code" = "409" ]; then \
	  echo "GTFS RT Ingestor Source enabled (HTTP $$code)"; \
	else echo "Failed to enable GTFS RT Ingestor Source (HTTP $$code)"; exit 1; fi

disable-gtfs-rt-ingestor-source:
	@code="$$( $(call delete_connector,$(INGESTOR_NAME)) )"; \
	if [ "$$code" = "204" ] || [ "$$code" = "404" ]; then \
	  echo "GTFS RT Ingestor Source disabled (HTTP $$code)"; \
	else echo "Failed to disable GTFS RT Ingestor Source (HTTP $$code)"; exit 1; fi

pause-gtfs-rt-ingestor-source:
	@code="$$( $(call pause_connector,$(INGESTOR_NAME)) )"; echo "Pause GTFS RT Ingestor Source: HTTP $$code"

resume-gtfs-rt-ingestor-source:
	@code="$$( $(call resume_connector,$(INGESTOR_NAME)) )"; echo "Resume GTFS RT Ingestor Source: HTTP $$code"

restart-gtfs-rt-ingestor-source:
	@code="$$( $(call restart_connector,$(INGESTOR_NAME)) )"; echo "Restart GTFS RT Ingestor Source: HTTP $$code"

.PHONY: enable-position-cache-redis-sink disable-position-cache-redis-sink pause-position-cache-redis-sink resume-position-cache-redis-sink restart-position-cache-redis-sink
enable-position-cache-redis-sink: connect-health
	@code="$$( $(call create_connector,$(POSITION_CACHE_JSON)) )"; \
	if [ "$$code" = "201" ] || [ "$$code" = "409" ]; then \
	  echo "Position Cache Redis Sink enabled (HTTP $$code)"; \
	else echo "Failed to enable Position Cache Redis Sink (HTTP $$code)"; exit 1; fi

disable-position-cache-redis-sink:
	@code="$$( $(call delete_connector,$(POSITION_CACHE_NAME)) )"; \
	if [ "$$code" = "204" ] || [ "$$code" = "404" ]; then \
	  echo "Position Cache Redis Sink disabled (HTTP $$code)"; \
	else echo "Failed to disable Position Cache Redis Sink (HTTP $$code)"; exit 1; fi

pause-position-cache-redis-sink:
	@code="$$( $(call pause_connector,$(POSITION_CACHE_NAME)) )"; echo "Pause Position Cache Redis Sink: HTTP $$code"

resume-position-cache-redis-sink:
	@code="$$( $(call resume_connector,$(POSITION_CACHE_NAME)) )"; echo "Resume Position Cache Redis Sink: HTTP $$code"

restart-position-cache-redis-sink:
	@code="$$( $(call restart_connector,$(POSITION_CACHE_NAME)) )"; echo "Restart Position Cache Redis Sink: HTTP $$code"

# ---- FileDump sink (опционально) ----
.PHONY: enable-filedump-sink disable-filedump-sink pause-filedump-sink resume-filedump-sink restart-filedump-sink
enable-filedump-sink: connect-health
	@code="$$( $(call create_connector,$(FILE_SINK_JSON)) )"; \
	if [ "$$code" = "201" ] || [ "$$code" = "409" ]; then \
	  echo "FileDump sink enabled (HTTP $$code)"; \
	else echo "Failed to enable FileDump sink (HTTP $$code)"; exit 1; fi

disable-filedump-sink:
	@code="$$( $(call delete_connector,$(FILE_SINK_NAME)) )"; \
	if [ "$$code" = "204" ] || [ "$$code" = "404" ]; then \
	  echo "FileDump sink disabled (HTTP $$code)"; \
	else echo "Failed to disable FileDump sink (HTTP $$code)"; exit 1; fi

pause-filedump-sink:
	@code="$$( $(call pause_connector,$(FILE_SINK_NAME)) )"; echo "Pause FileDump sink: HTTP $$code"

resume-filedump-sink:
	@code="$$( $(call resume_connector,$(FILE_SINK_NAME)) )"; echo "Resume FileDump sink: HTTP $$code"

restart-filedump-sink:
	@code="$$( $(call restart_connector,$(FILE_SINK_NAME)) )"; echo "Restart FileDump sink: HTTP $$code"

.PHONY: enable-all disable-all help
enable-all: enable-gtfs-rt-ingestor-source enable-position-cache-redis-sink
	@echo "All connectors enabled."

disable-all: disable-gtfs-rt-ingestor-source disable-position-cache-redis-sink disable-filedump-sink
	@echo "All connectors disabled."

help:
	@echo "Commands:"
	@echo "  up / down / clean-volumes / restart / logs / ps"
	@echo "  connect-health, plugins, connectors, status NAME=<name>"
	@echo "  enable-gtfs-rt-ingestor-source | disable-gtfs-rt-ingestor-source | pause-gtfs-rt-ingestor-source | resume-gtfs-rt-ingestor-source | restart-gtfs-rt-ingestor-source"
	@echo "  enable-position-cache-redis-sink | disable-position-cache-redis-sink | pause-position-cache-redis-sink | resume-position-cache-redis-sink | restart-position-cache-redis-sink"
	@echo "  enable-filedump-sink | disable-filedump-sink | pause-filedump-sink | resume-filedump-sink | restart-filedump-sink"
	@echo "  enable-all / disable-all"
