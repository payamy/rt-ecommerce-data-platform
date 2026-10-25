# Real-Time E-Commerce Data Platform

A portfolio project for building a data platform that simulates e-commerce activity, streams events through Kafka, and processes them for real-time analytics and fraud detection.

The project is designed to demonstrate practical data engineering concepts, including event generation, distributed messaging, stream processing, data modeling, and analytical APIs.

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
      Spark Structured        Raw Event Storage
        Streaming              (planned)
              |
              v
       Real-Time Processing
              |
       +------+------+
       |             |
       v             v
   ClickHouse     Delta Lake
   (Analytics)    (Historical Data)
       |             |
       +------+------+
              |
              v
           FastAPI
              |
              v
         API Consumers
```

This diagram represents the intended architecture. The generator, Kafka infrastructure, PySpark fraud transformations, and Kafka-to-ClickHouse processing application are implemented. Historical storage and API components are planned and may change as the project develops.

### Technology stack

| Component            | Technology                        | Purpose                           |
| -------------------- | --------------------------------- | --------------------------------- |
| Event generation     | Python, asyncio                   | Simulate concurrent user sessions |
| Messaging            | Apache Kafka                      | Transport event streams           |
| Kafka client         | `confluent-kafka`                 | Publish events to Kafka           |
| Stream processing    | Apache Spark Structured Streaming | Process events in near real time  |
| Historical storage   | Delta Lake, MinIO                 | Store and manage historical data  |
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
* `docker-compose-clickhouse.yaml`: Local persistent ClickHouse service for processing results.
* `docker-compose.yml`: Defines the local Kafka infrastructure.
