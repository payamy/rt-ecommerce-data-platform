from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from src.lakehouse.models import bronze_events, daily_payments, payment_events
from src.lakehouse.pipeline import (
    LakehousePaths, initialize_tables, merge_payments, refresh_reports,
    start_bronze, start_gold, start_silver,
)


class LakehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (SparkSession.builder.master("local[2]").appName("lakehouse-tests")
                   .config("spark.ui.enabled", "false")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def event(identifier, kind="payment_completed", amount=12.34, day=26):
        return dict(event_id=identifier, event_type=kind, amount=amount,
                    user_id=1, session_id="session", payment_method="card",
                    event_time=f"2026-10-{day:02d}T12:00:00Z")

    def kafka(self, records):
        return self.spark.createDataFrame(
            [("ecommerce-events", 0, i, None, json.dumps(record).encode(),
              datetime(2026, 10, 27)) for i, record in enumerate(records)],
            "topic string, partition int, offset long, key binary, value binary, timestamp timestamp")

    def test_payment_validation_and_decimal_reports(self):
        records = [self.event("attempt", "payment_attempted"), self.event("completed"),
                   self.event("failed", "payment_failed"),
                   self.event("refund", "refund_created", amount=2.34),
                   self.event("invalid-time") | {"event_time": "invalid"},
                   self.event("negative", amount=-1), self.event("zero", amount=0),
                   self.event("browse", "product_viewed"),
                   self.event("missing-user") | {"user_id": None}]
        bronze = bronze_events(self.kafka(records))
        self.assertEqual(bronze.count(), len(records))
        payments = payment_events(bronze)
        self.assertEqual(payments.count(), 4)
        report = daily_payments(payments).first()
        self.assertEqual(report.completed_amount, Decimal("12.34"))
        self.assertEqual(report.net_amount, Decimal("10.00"))
        self.assertEqual(report.completion_rate, 0.5)
        attempt_only = payment_events(bronze_events(self.kafka([self.event("a", "payment_attempted")])))
        self.assertIsNone(daily_payments(attempt_only).first().completion_rate)

    def test_initialize_rejects_non_delta_directory_without_deleting_files(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = LakehousePaths(directory, f"{directory}/checkpoints")
            bronze = Path(paths.bronze)
            bronze.mkdir(parents=True)
            marker = bronze / "existing-data.txt"
            marker.write_text("preserve me")
            with self.assertRaisesRegex(ValueError, "Existing path is not a Delta table"):
                initialize_tables(self.spark, paths)
            self.assertEqual(marker.read_text(), "preserve me")

    def test_three_streams_late_events_duplicates_restart_and_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = root / "input"
            incoming.mkdir()
            paths = LakehousePaths(str(root / "lake"), str(root / "checkpoints"))
            initialize_tables(self.spark, paths)
            source = self.spark.readStream.schema("value string, offset long").json(str(incoming))
            kafka = source.select(
                F.lit("ecommerce-events").alias("topic"), F.lit(0).alias("partition"),
                "offset", F.lit(None).cast("binary").alias("key"),
                F.col("value").cast("binary").alias("value"),
                F.lit("2026-10-27T00:00:00Z").cast("timestamp").alias("timestamp"))
            queries = []

            def send(filename, records):
                (incoming / filename).write_text("\n".join(
                    json.dumps({"value": json.dumps(record), "offset": index})
                    for index, record in enumerate(records)))
                for query in queries:
                    query.processAllAvailable()

            try:
                queries.append(start_bronze(kafka, paths, 1))
                queries.append(start_silver(self.spark, paths, 1))
                queries.append(start_gold(self.spark, paths, 1))
                send("first.json", [self.event("attempt", "payment_attempted"),
                                    self.event("completed"), self.event("completed"),
                                    self.event("new-day", day=27), {"malformed": True}])
                self.assertEqual(self.spark.read.format("delta").load(paths.bronze).count(), 5)
                initialize_tables(self.spark, paths)
                self.assertEqual(self.spark.read.format("delta").load(paths.bronze).count(), 5)
                self.assertEqual(self.spark.read.format("delta").load(paths.silver).count(), 3)
                # Late old-date event and a repeated ID after a silver checkpoint restart.
                queries[1].stop()
                queries[1] = start_silver(self.spark, paths, 1)
                send("late.json", [self.event("completed"),
                                   self.event("late-refund", "refund_created", amount=2.34)])
                silver = self.spark.read.format("delta").load(paths.silver)
                self.assertEqual(silver.count(), 4)
                old = (self.spark.read.format("delta").load(paths.gold)
                       .where("event_date = '2026-10-26'").first())
                self.assertEqual(old.completed_count, 1)
                self.assertEqual(old.refund_count, 1)
                self.assertEqual(old.net_amount, Decimal("10.00"))
                # Simulate foreachBatch retry after the table commit.
                merge_payments(self.spark.read.format("delta").load(paths.bronze), 99, paths.silver)
                refresh_reports(silver, 99, paths)
                refresh_reports(silver, 99, paths)
                self.assertEqual(self.spark.read.format("delta").load(paths.silver).count(), 4)
                actual = self.spark.read.format("delta").load(paths.gold).collect()
                expected = daily_payments(self.spark.read.format("delta").load(paths.silver)).collect()
                key = lambda row: (row.event_date, row.payment_method)
                self.assertEqual({key(row): row.asDict() for row in actual},
                                 {key(row): row.asDict() for row in expected})
            finally:
                for query in reversed(queries):
                    query.stop()
