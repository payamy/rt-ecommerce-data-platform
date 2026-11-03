"""Independently checkpointed Delta streams with replay-safe table writes."""

from dataclasses import dataclass
from functools import partial

from delta.tables import DeltaTable

from .models import bronze_events, daily_payments, payment_events


@dataclass(frozen=True)
class LakehousePaths:
    """Paths under the warehouse root matching ecommerce's default table layout.

    Existing Hive database/table locations may override this layout; migrate
    existing data and catalog entries before switching a running pipeline.
    """
    root: str
    checkpoints: str

    def table(self, name):
        return f"{self.root.rstrip('/')}/ecommerce.db/{name}"

    def checkpoint(self, stage):
        return f"{self.checkpoints.rstrip('/')}/{stage}"

    @property
    def bronze(self):
        return self.table("bronze_events")

    @property
    def silver(self):
        return self.table("payment_events")

    @property
    def gold(self):
        return self.table("daily_payments")


def initialize_tables(spark, paths):
    """Create empty tables in dependency order before starting downstream reads."""
    kafka_schema = "topic string, partition int, offset long, key binary, value binary, timestamp timestamp"
    bronze = bronze_events(spark.createDataFrame([], kafka_schema))
    silver = payment_events(bronze)
    gold = daily_payments(silver)
    for frame, path, partition in (
        (bronze, paths.bronze, "ingestion_date"),
        (silver, paths.silver, "event_date"),
        (gold, paths.gold, "event_date"),
    ):
        # Preserve existing Delta tables, but fail on unrelated/incomplete
        # directories instead of silently skipping their initialization.
        if not DeltaTable.isDeltaTable(spark, path):
            hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
            filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
            if filesystem.exists(hadoop_path):
                raise ValueError(f"Existing path is not a Delta table: {path}")
            frame.limit(0).write.format("delta").mode("errorifexists").partitionBy(partition).save(path)


def merge_payments(batch, batch_id, path):
    # Kafka duplicates share an immutable event_id. Deduplicate within a batch
    # as well as against the full silver table, with no late-event cutoff.
    payments = payment_events(batch).dropDuplicates(["event_id"])
    (DeltaTable.forPath(batch.sparkSession, path).alias("target")
     .merge(payments.alias("source"), "target.event_id = source.event_id")
     .whenNotMatchedInsertAll().execute())


def refresh_reports(batch, batch_id, paths):
    """Recompute affected dates from silver, rather than add counts on retries."""
    dates = batch.select("event_date").distinct()
    # Snapshot the silver version for a consistent report refresh. Future inserts
    # are either already reflected or will trigger another refresh of their date.
    spark = batch.sparkSession
    version = DeltaTable.forPath(spark, paths.silver).history(1).select("version").first()[0]
    silver = spark.read.format("delta").option("versionAsOf", version).load(paths.silver)
    reports = daily_payments(silver.join(dates, "event_date", "left_semi"))
    (DeltaTable.forPath(spark, paths.gold).alias("target")
     .merge(reports.alias("source"),
            "target.event_date = source.event_date AND target.payment_method = source.payment_method")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def start_bronze(kafka, paths, trigger_seconds):
    return (bronze_events(kafka).writeStream.format("delta").outputMode("append")
            .queryName("lakehouse-bronze")
            .option("checkpointLocation", paths.checkpoint("bronze"))
            .partitionBy("ingestion_date")
            .trigger(processingTime=f"{trigger_seconds} seconds").start(paths.bronze))


def start_silver(spark, paths, trigger_seconds):
    bronze = spark.readStream.format("delta").load(paths.bronze)
    return (bronze.writeStream.queryName("lakehouse-silver").outputMode("append")
            .option("checkpointLocation", paths.checkpoint("silver"))
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .foreachBatch(partial(merge_payments, path=paths.silver)).start())


def start_gold(spark, paths, trigger_seconds):
    # Silver uses insert-only MERGE, so it is an append-only streaming source.
    silver = spark.readStream.format("delta").load(paths.silver)
    return (silver.writeStream.queryName("lakehouse-gold").outputMode("append")
            .option("checkpointLocation", paths.checkpoint("gold"))
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .foreachBatch(partial(refresh_reports, paths=paths)).start())
