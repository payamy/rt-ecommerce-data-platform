"""Run with python -m src.processing.main from the repository root."""

import argparse
from functools import partial
import os

import pyspark
from pyspark.sql import SparkSession

from src.processing.clickhouse import ClickHouseConfig, initialize_table, write_batch
from src.processing.fraud import FraudConfig, label_sessions, parse_events


def arguments(argv=None):
    parser = argparse.ArgumentParser(description="Kafka → session fraud labels → ClickHouse")
    parser.add_argument("--bootstrap-servers", default=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "ecommerce-events"))
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--checkpoints", required=True, help="Stable, durable checkpoints path/URI")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=100000)
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"))
    parser.add_argument("--clickhouse-database", default=os.getenv("CLICKHOUSE_DATABASE", "ecommerce"))
    parser.add_argument("--clickhouse-table", default=os.getenv("CLICKHOUSE_TABLE", "session_fraud"))
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--slide-seconds", type=int, default=30)
    parser.add_argument("--lateness-seconds", type=int, default=120)
    parser.add_argument("--min-attempts", type=int, default=6)
    parser.add_argument("--min-failures", type=int, default=5)
    args = parser.parse_args(argv)
    if args.trigger_seconds <= 0 or args.max_offsets_per_trigger <= 0:
        parser.error("trigger interval and maximum offsets must be positive")
    return args


def start_processing(spark, args, clickhouse, fraud):
    initialize_table(clickhouse)
    payloads = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap_servers)
                .option("subscribe", args.topic)
                .option("startingOffsets", args.starting_offsets)
                .option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                .load())
    labels = label_sessions(parse_events(payloads), fraud)
    return (labels.writeStream.queryName("session-fraud-to-clickhouse")
            .outputMode("append")
            .option("checkpointLocation", args.checkpoint)
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .foreachBatch(partial(write_batch, config=clickhouse))
            .start())


def main(argv=None):
    args = arguments(argv)
    clickhouse = ClickHouseConfig(
        url=args.clickhouse_url, database=args.clickhouse_database,
        table=args.clickhouse_table,
        user=os.getenv("CLICKHOUSE_USER", "ecommerce"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "ecommerce_local"),
    )
    fraud = FraudConfig(window_seconds=args.window_seconds, slide_seconds=args.slide_seconds,
                        lateness_seconds=args.lateness_seconds,
                        min_attempts=args.min_attempts, min_failures=args.min_failures)
    # Match the Kafka connector to the installed Spark version (Spark 3.5 / Scala 2.12).
    spark = (SparkSession.builder.appName("ecommerce-fraud-processing")
             .config("spark.jars.packages",
                     f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    query = None
    try:
        query = start_processing(spark, args, clickhouse, fraud)
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
