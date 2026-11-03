# Real-Time E-Commerce Data Platform

A portfolio project for building a data platform that simulates e-commerce activity, streams events through Kafka, and processes them for real-time analytics and fraud detection.

The project is designed to demonstrate practical data engineering concepts, including event generation, distributed messaging, stream processing, data modeling, and analytical APIs.

## Running the platform

With Docker Compose v2, Java 17, and requirements installed in `venv`, run:

```bash
make up                                     # ARM64 by default
make up LAKEHOUSE_PLATFORM=linux/amd64       # Intel/AMD
make status
make logs
make stop                                   # Stop apps; keep infrastructure
make down                                   # Stop everything; preserve data
```

Startup runs Kafka and topic creation, ClickHouse, the metastore and Spark,
lakehouse streaming, table registration for JDBC, fraud processing, and finally
the event generator. It waits for infrastructure readiness and initial Delta
commits before registering tables. Use `make infra` for infrastructure only and
`make help` for individual subsystem commands. Set `WAIT_TIMEOUT=1200` to adjust
the default 900-second startup waits.

The generator and fraud processor run on the host, with logs and PID files in
`.run/`. These commands manage only host processes they started; stop manually
launched copies before using `make up` to avoid duplicate writers. Failed
startup leaves started services available for inspection; rerun after fixing
the error. Use the same platform override for subsequent Make commands.

## Project Overview

The platform simulates user activity across an e-commerce application. Synthetic events are generated from user sessions and published to Kafka, providing a stream of realistic business activity for downstream processing.

The long-term goal is to build an end-to-end data platform that supports both real-time and historical analytics.

### Key objectives

* Generate realistic e-commerce events using Python.
* Simulate concurrent user sessions and time-dependent behavior.
* Publish events to Apache Kafka.
* Introduce duplicate events and suspicious activity patterns for testing data quality and fraud detection.
* Process streaming events with Apache Spark Structured Streaming.
* Store historical and analytical data in appropriate storage systems.
* Expose processed data through a FastAPI backend.

## Architecture

### Target architecture

```text
              Synthetic Data Generator
                         |
                         v
                    Apache Kafka
                         |
              +----------+----------+
              |                     |
              v                     v
      Spark Structured        Delta Lakehouse
        Streaming             Bronze: raw events
              |
              v
       Real-Time Processing
              |
       +------+------+
       |             |
       v             v
   ClickHouse     Silver: payment events
   (Fraud)        Gold: daily reports
       |             |
       +------+------+
              |
              v
           FastAPI
              |
              v
         API Consumers
```

The generator, Kafka infrastructure, fraud-to-ClickHouse application, and Delta bronze/silver/gold lakehouse are implemented. The lakehouse runs as an independent Kafka consumer with a checkpoint per layer. The API is planned.

### Technology stack

| Component            | Technology                        | Purpose                           |
| -------------------- | --------------------------------- | --------------------------------- |
| Event generation     | Python, asyncio                   | Simulate concurrent user sessions |
| Messaging            | Apache Kafka                      | Transport event streams           |
| Kafka client         | `confluent-kafka`                 | Publish events to Kafka           |
| Stream processing    | Apache Spark Structured Streaming | Process events in near real time  |
| Lakehouse            | Delta Lake                        | Bronze events, silver payments, gold reports |
| Analytical database  | ClickHouse                        | Serve analytical queries          |
| API                  | FastAPI                           | Expose processed data             |
| Local infrastructure | Docker Compose                    | Run services locally              |

## Project Structure

The repository is organized around the generator and its supporting infrastructure. The structure may evolve as additional components are implemented.

```text
rt-ecommerce-data-platform/
├── src/
│   ├── generator/
│   │   ├── generator.py
│   │   └── session.py
│   └── producer/
│       └── kafka_producer.py
├── docker-compose.yml
├── README.md
└── ...
```

* `src/generator/generator.py`: Generates individual e-commerce events.
* `src/generator/session.py`: Coordinates events into user sessions and simulates delays between them.
* `src/producer/kafka_producer.py`: Encapsulates Kafka producer configuration and event publishing.
* `src/processing/`: Source-independent PySpark streaming and batch session fraud labeling. See [processing usage and semantics](src/processing/README.md).
* `src/processing/main.py`: Kafka-to-ClickHouse streaming application with durable checkpoints.
* `src/lakehouse/`: Kafka-to-Delta bronze ingestion, deduplicated silver payment events, and daily gold reporting. See [lakehouse setup and models](src/lakehouse/README.md).
* `docker-compose-clickhouse.yaml`: Local persistent ClickHouse service for processing results.
* `docker-compose-lakehouse.yaml`: Shared Hive metastore, Spark standalone cluster, and Delta-enabled Spark Thrift Server for JDBC. See [setup and connection instructions](docker/lakehouse/README.md).
* `docker-compose.yml`: Defines the local Kafka infrastructure.
