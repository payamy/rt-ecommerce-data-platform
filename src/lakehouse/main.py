"""Kafka → bronze → silver payment events → daily gold reports."""

import argparse
import os

from delta import configure_spark_with_delta_pip
import pyspark
from pyspark.sql import SparkSession

from .pipeline import LakehousePaths, initialize_tables, start_bronze, start_gold, start_silver


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Lakehouse storage path/URI")
    parser.add_argument("--checkpoints", required=True, help="Durable checkpoint root (separate from tables)")
    parser.add_argument("--stage", choices=("all", "bronze", "silver", "gold"), default="all")
    parser.add_argument("--bootstrap-servers", default=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "ecommerce-events"))
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=100000)
    args = parser.parse_args(argv)
    if args.trigger_seconds <= 0 or args.max_offsets_per_trigger <= 0:
        parser.error("trigger interval and maximum offsets must be positive")
    return args


def main(argv=None):
    args = arguments(argv)
    paths = LakehousePaths(args.root, args.checkpoints)
    builder = (SparkSession.builder.appName("ecommerce-lakehouse")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC"))
    packages = ([f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"]
                if args.stage in ("all", "bronze") else [])
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    queries = []
    try:
        initialize_tables(spark, paths)
        if args.stage in ("all", "bronze"):
            kafka = (spark.readStream.format("kafka")
                     .option("kafka.bootstrap.servers", args.bootstrap_servers)
                     .option("subscribe", args.topic)
                     .option("startingOffsets", args.starting_offsets)
                     .option("failOnDataLoss", "true")
                     .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger).load())
            queries.append(start_bronze(kafka, paths, args.trigger_seconds))
        if args.stage in ("all", "silver"):
            queries.append(start_silver(spark, paths, args.trigger_seconds))
        if args.stage in ("all", "gold"):
            queries.append(start_gold(spark, paths, args.trigger_seconds))
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for query in reversed(queries):
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
