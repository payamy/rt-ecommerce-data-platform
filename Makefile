SHELL := /bin/bash
.DEFAULT_GOAL := help
# Keep startup ordered even when invoked with make -j.
.NOTPARALLEL:

LAKEHOUSE_PLATFORM ?= linux/arm64
export LAKEHOUSE_PLATFORM
export LAKEHOUSE_KAFKA_NETWORK := rt-ecommerce-data-platform_kafka-net
WAIT_TIMEOUT ?= 900
KAFKA = docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-kafka.yaml
CLICKHOUSE = docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-clickhouse.yaml
LAKEHOUSE = docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-lakehouse.yaml

.PHONY: help up infra kafka clickhouse lakehouse lakehouse-app register apps generator processing status logs stop down
help:
	@echo 'make up             Start infrastructure, streams, table registration, generator'
	@echo 'make infra          Start Kafka, ClickHouse, metastore, Spark, JDBC'
	@echo 'make apps           Start apps after infrastructure (also starts infrastructure)'
	@echo 'make status|logs    Inspect containers and host app processes/logs'
	@echo 'make stop           Stop apps; leave infrastructure running'
	@echo 'make down           Stop everything; preserve data and checkpoints'
	@echo 'Override: make up LAKEHOUSE_PLATFORM=linux/amd64 WAIT_TIMEOUT=1200'

up: apps

infra: kafka clickhouse lakehouse

kafka:
	$(KAFKA) up -d
	@bash scripts/apps.sh wait-kafka "$(WAIT_TIMEOUT)"
	$(KAFKA) exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:19092 --create --if-not-exists --topic ecommerce-events --partitions 6 --replication-factor 3

clickhouse:
	$(CLICKHOUSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

lakehouse:
	$(LAKEHOUSE) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

lakehouse-app: infra
	$(LAKEHOUSE) --profile app up -d --no-deps lakehouse-app

register: lakehouse-app
	@bash scripts/apps.sh wait-tables "$(WAIT_TIMEOUT)"
	$(LAKEHOUSE) exec -T spark-thriftserver /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive --force=false -f /workspace/register-tables.sql

apps: register processing generator

processing: clickhouse kafka
	@bash scripts/apps.sh start processing

generator: kafka
	@bash scripts/apps.sh start generator

status:
	$(KAFKA) ps
	$(CLICKHOUSE) ps
	$(LAKEHOUSE) --profile app ps
	@bash scripts/apps.sh status

logs:
	$(LAKEHOUSE) --profile app logs --tail=40
	@bash scripts/apps.sh logs

stop:
	@bash scripts/apps.sh stop generator
	@bash scripts/apps.sh stop processing
	$(LAKEHOUSE) --profile app stop lakehouse-app

down: stop
	$(LAKEHOUSE) --profile app down
	docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-kafka.yaml -f docker-compose-clickhouse.yaml down
