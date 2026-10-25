"""Shared Structured Streaming and batch transformations; no I/O is started here."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, types as T


EVENT_SCHEMA = T.StructType([
    T.StructField(name, dtype, True)
    for name, dtype in [
        ("event_id", T.StringType()), ("event_type", T.StringType()),
        ("event_time", T.StringType()), ("user_id", T.LongType()),
        ("session_id", T.StringType()), ("device_id", T.StringType()),
        ("product_id", T.LongType()), ("merchant_id", T.LongType()),
        ("amount", T.DoubleType()), ("payment_method", T.StringType()),
        ("failure_reason", T.StringType()),
    ]
])


@dataclass(frozen=True)
class FraudConfig:
    window_seconds: int = 60
    slide_seconds: int = 10
    lateness_seconds: int = 120
    min_attempts: int = 6
    min_failures: int = 5

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.slide_seconds > self.window_seconds:
            raise ValueError("slide_seconds must not exceed window_seconds")


def parse_events(payloads: DataFrame, column: str = "value") -> DataFrame:
    """Decode JSON string/binary payloads into the generator's event columns.

    Invalid JSON becomes null fields and is excluded by label_sessions.
    Callers retaining rejected records should keep the original payload DataFrame.
    """
    return payloads.select(
        F.from_json(F.col(column).cast("string"), EVENT_SCHEMA).alias("event")
    ).select("event.*")


def label_sessions(events: DataFrame, config: FraudConfig | None = None) -> DataFrame:
    """Label each (user_id, session_id, window) using deduplicated events.

    Accepts a static or streaming DataFrame with EVENT_SCHEMA columns. Returns
    a lazy DataFrame; callers own sources, sinks, checkpoints and query lifecycle.
    Streaming callers must use append output mode for the chained stateful plan.
    """
    config = config or FraudConfig()
    # try_cast tolerates malformed timestamps even when Spark ANSI mode is on.
    clean = events.withColumn("event_time", F.expr("try_cast(event_time as timestamp)"))
    clean = clean.where(
        F.col("event_time").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_id").isNotNull() & (F.length(F.trim("event_id")) > 0)
        & F.col("session_id").isNotNull() & (F.length(F.trim("session_id")) > 0)
        & F.col("event_type").isin(
            "user_registered", "product_viewed", "product_added_to_cart",
            "checkout_started", "payment_attempted", "payment_completed",
            "payment_failed", "refund_created",
        )
    )
    if clean.isStreaming:
        clean = clean.withWatermark("event_time", f"{config.lateness_seconds} seconds")
        clean = clean.dropDuplicatesWithinWatermark(["event_id"])
    else:
        clean = clean.dropDuplicates(["event_id"])

    def count_type(event_type):
        return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))

    features = clean.groupBy(
        "user_id", "session_id",
        F.window("event_time", f"{config.window_seconds} seconds",
                 f"{config.slide_seconds} seconds"),
    ).agg(
        F.count("*").alias("event_count"),
        count_type("payment_attempted").alias("payment_attempts"),
        count_type("payment_failed").alias("payment_failures"),
        count_type("payment_completed").alias("payment_completions"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    )
    reasons = F.filter(F.array(
        F.when(F.col("payment_attempts") >= config.min_attempts,
               F.lit("high_payment_velocity")),
        F.when(F.col("payment_failures") >= config.min_failures,
               F.lit("repeated_payment_failures")),
    ), lambda reason: reason.isNotNull())
    return features.withColumn("fraud_reasons", reasons).withColumn(
        "is_fraudulent", F.size("fraud_reasons") > 0
    ).select(
        "user_id", "session_id", F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"), "event_count", "payment_attempts",
        "payment_failures", "payment_completions", "first_event_time",
        "last_event_time", "fraud_reasons", "is_fraudulent",
    )
