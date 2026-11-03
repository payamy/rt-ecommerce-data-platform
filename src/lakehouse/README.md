# Lakehouse

Three Delta Lake tables run as independently checkpointed Structured Streaming
queries. Delta 3.3 supports the project's Spark 3.5 runtime. Storage is configured
by path; local disk works for development. A metastore and object store are not
required to run locally.

```text
Kafka → ecommerce.db/bronze_events → ecommerce.db/payment_events → ecommerce.db/daily_payments
```

**Bronze** appends every Kafka message with its original binary key/value,
topic, partition, offset, broker timestamp, ingestion time and ingestion date.
It preserves invalid payloads and application duplicates for audit and replay.
The Delta sink and its checkpoint prevent a committed Kafka microbatch from
being appended again on normal restart. Partitioning uses UTC ingestion date.

**Silver** is a payment-event fact table: one row per immutable `event_id`.
It includes attempted, completed, failed and refund events, typed UTC event time,
event date, decimal amount, identifiers, payment method, failure reason and Kafka
lineage. Insert-only MERGE deduplicates within and across microbatches without a
watermark cutoff, so late events are accepted. Duplicate IDs must have identical
payloads; the first committed row wins. Missing payment methods become `unknown`.
Non-payment events and rows with invalid IDs, timestamps, users or non-positive/
invalid amounts are excluded from silver; their original records remain in bronze.

The generator has no payment transaction/attempt ID, so matching outcomes to
attempts would be ambiguous when a session retries. Silver deliberately keeps
event grain. Add an explicit payment ID upstream before creating a transaction
fact model. Merchant IDs can be null on payments, so reports do not infer merchants.

**Gold** has one row per `(event_date, payment_method)` with attempt, completed,
failed and refund counts and their corresponding amounts. It also includes
`net_amount = completed_amount - refunded_amount` and
`completion_rate = completed_count / (completed_count + failed_count)` (null when
there are no outcomes). Refunds are reported on refund event date, rather than
restating the original sale date. These are event metrics, not distinct purchases.
The input has no currency; monetary totals assume the generator's single currency.
Add currency to the input and grouping before ingesting multiple currencies.

Each new silver microbatch causes gold to recompute affected dates from a pinned
silver snapshot and MERGE replacement totals. Retries cannot add counts twice,
and old dates refresh when late payments arrive. This trades scans of affected
date partitions for simple recovery and avoids unbounded streaming aggregation
state. Daily partitions may need a different layout as data volume grows.

## Run locally

For a shared Hive metastore, Spark standalone cluster, and JDBC access through
Spark Thrift Server, see the [Docker lakehouse setup](../../docker/lakehouse/README.md).
The following instructions run Spark directly on the host.

From the repository root, with Java 17 and Python 3.11:

```bash
venv/bin/python -m pip install -r requirements.txt
docker compose --env-file /dev/null -f docker-compose-kafka.yaml up -d
export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS; use your JVM path elsewhere
unset SPARK_HOME
export PYSPARK_PYTHON="$PWD/venv/bin/python"
export PYSPARK_SUBMIT_ARGS='--master local[2] pyspark-shell'
venv/bin/python -m src.lakehouse.main \
  --root "$PWD/lakehouse-data" \
  --checkpoints "$PWD/checkpoints/lakehouse"
```

Start the event generator separately. Kafka defaults match the existing local
cluster/topic; `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` override them.
`--starting-offsets earliest` is the default for a new checkpoint. On restart,
offsets come from checkpoints. Missing Kafka data fails ingestion.
The first launch resolves Delta and Kafka JVM dependencies from Maven.

`--stage bronze|silver|gold` runs one stage instead of all three. Use a single
writer per stage/table, with its own stable checkpoint subdirectory. The default
10-second microbatch interval applies to each stage; end-to-end latency includes
all three stages and processing time. There is no late-data finalization wait.
Any stage failure in `--stage all` stops the application; restart with the same
paths to recover each query independently.

Tables and checkpoints must be durable and shared by all workers for distributed
execution. For object storage, supply the relevant Hadoop connector, credentials
and Delta LogStore configuration for your storage provider. No MinIO/S3 deployment
is assumed or configured. All workers need the project's `src` Python package.

Keep bronze and silver append-only. Silver corrections/deletes require an explicit
downstream rebuild strategy; this pipeline does not silently skip change commits.
For a rebuild under changed rules, use fresh output roots and checkpoints. To
rebuild silver/gold from existing bronze, copy bronze to the new root using Delta
or run stages against independently configured paths in the Python API. Do not
reset a bronze checkpoint while retaining its target and expect source deduplication.
Retain source Delta logs/data long enough for downstream readers; manage compaction
and VACUUM separately, accounting for the slowest stream. Table commits are atomic,
but the three layers are eventually consistent rather than a shared transaction.

## Query reports

With the Delta-enabled Spark session created by the application:

```python
spark.read.format("delta").load("lakehouse-data/ecommerce.db/daily_payments").orderBy(
    "event_date", "payment_method"
).show()
```

Transformations in `models.py` also accept static DataFrames for batch use.
`merge_payments` accepts a static bronze batch, and `refresh_reports` accepts
static silver rows identifying dates to recompute. The fraud/ClickHouse application
remains an independent Kafka consumer and does not need to run for the lakehouse.

References: [Delta/Spark compatibility](https://docs.delta.io/releases/),
[streaming reads and writes](https://docs.delta.io/delta-streaming/),
[insert-only MERGE and streaming deduplication](https://docs.delta.io/delta-update/).
