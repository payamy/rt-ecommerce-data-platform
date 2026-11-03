"""Pure DataFrame transformations shared by streaming microbatches and batch."""

from pyspark.sql import DataFrame, functions as F

from src.processing.fraud import EVENT_SCHEMA


PAYMENT_TYPES = ("payment_attempted", "payment_completed", "payment_failed", "refund_created")


def bronze_events(kafka: DataFrame) -> DataFrame:
    """Preserve every Kafka record, including malformed payloads and duplicates."""
    return kafka.select(
        "topic", "partition", "offset", "key", "value",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingestion_date", F.to_date("ingested_at"))


def payment_events(bronze: DataFrame) -> DataFrame:
    """Validated payment-event fact rows; do not infer transaction identities."""
    # Decode money directly as decimal, avoiding floating-point sums in reports.
    schema = EVENT_SCHEMA.jsonValue()
    for field in schema["fields"]:
        if field["name"] == "amount":
            field["type"] = "decimal(18,2)"
    from pyspark.sql.types import StructType

    events = bronze.select(
        F.from_json(F.col("value").cast("string"), StructType.fromJson(schema)).alias("event"),
        "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
    ).select("event.*", "topic", "partition", "offset", "kafka_timestamp", "ingested_at")
    events = events.withColumn("event_time", F.expr("try_cast(event_time as timestamp)"))
    valid = (
        F.col("event_type").isin(*PAYMENT_TYPES)
        & F.col("event_time").isNotNull() & F.col("user_id").isNotNull()
        & F.col("event_id").isNotNull() & (F.length(F.trim("event_id")) > 0)
        & F.col("session_id").isNotNull() & (F.length(F.trim("session_id")) > 0)
        & F.col("amount").isNotNull() & (F.col("amount") > 0)
    )
    return events.where(valid).select(
        "event_id", "event_type", "event_time", F.to_date("event_time").alias("event_date"),
        "user_id", "session_id", "device_id", "merchant_id", "amount",
        F.coalesce("payment_method", F.lit("unknown")).alias("payment_method"),
        "failure_reason", "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
    )


def daily_payments(payments: DataFrame) -> DataFrame:
    """Daily event metrics; completion rate uses outcome count as denominator."""
    def count(kind):
        return F.sum(F.when(F.col("event_type") == kind, 1).otherwise(0))

    def amount(kind):
        return F.sum(F.when(F.col("event_type") == kind, F.col("amount"))
                     .otherwise(F.lit(0).cast("decimal(18,2)")))

    summary = payments.groupBy("event_date", "payment_method").agg(
        count("payment_attempted").alias("attempt_count"),
        count("payment_completed").alias("completed_count"),
        count("payment_failed").alias("failed_count"),
        count("refund_created").alias("refund_count"),
        amount("payment_attempted").alias("attempted_amount"),
        amount("payment_completed").alias("completed_amount"),
        amount("payment_failed").alias("failed_amount"),
        amount("refund_created").alias("refunded_amount"),
    )
    outcomes = F.col("completed_count") + F.col("failed_count")
    return summary.withColumn(
        "completion_rate", F.when(outcomes > 0, F.col("completed_count") / outcomes)
    ).withColumn("net_amount", F.col("completed_amount") - F.col("refunded_amount"))
