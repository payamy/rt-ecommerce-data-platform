## Generator

The generator simulates user behavior in an e-commerce application. Rather than publishing arbitrary independent records, it creates sequences of related events that represent user sessions.

For example, a session might contain the following sequence:

```text
user_registered
       |
       v
product_viewed
       |
       v
product_added_to_cart
       |
       v
checkout_started
       |
       v
payment_attempted
       |
       +-------> payment_completed
       |
       +-------> payment_failed
```

The exact sequence depends on the generated session behavior. Not every session needs to complete a purchase.

### Event generation

The `EventGenerator` is responsible for constructing individual events.

Events share a common schema and can include the following fields:

| Field            | Description                                         |
| ---------------- | --------------------------------------------------- |
| `event_id`       | Unique identifier for an event                      |
| `event_type`     | Type of activity being recorded                     |
| `event_time`     | Timestamp associated with the event                 |
| `user_id`        | Identifier of the simulated user                    |
| `session_id`     | Identifier connecting events from a session         |
| `device_id`      | Identifier of the simulated device                  |
| `product_id`     | Product involved, when applicable                   |
| `merchant_id`    | Merchant associated with the product or transaction |
| `amount`         | Monetary value, when applicable                     |
| `payment_method` | Payment method, when applicable                     |
| `failure_reason` | Reason for a failed payment, when applicable        |

Fields that do not apply to a particular event can be omitted or represented as `null`, depending on the event serialization implementation.

Events are represented using a Python dataclass and serialized to JSON through the event's `to_json()` method.

### Event types

The generator supports the following event types in its design:

| Event type              | Meaning                        |
| ----------------------- | ------------------------------ |
| `user_registered`       | A user creates an account      |
| `product_viewed`        | A user views a product         |
| `product_added_to_cart` | A product is added to a cart   |
| `checkout_started`      | A user begins checkout         |
| `payment_attempted`     | A payment attempt is initiated |
| `payment_completed`     | A payment succeeds             |
| `payment_failed`        | A payment fails                |
| `refund_created`        | A refund is initiated          |

These event types provide a foundation for downstream analytics, such as conversion funnels, payment success rates, merchant performance, and suspicious transaction detection.

### Session generation

The `SessionGenerator` coordinates multiple events into a user session.

Its responsibilities include:

* Creating the sequence of events for a session.
* Reusing relevant session context, such as the session identifier.
* Coordinating event generation and publication.
* Introducing delays between events to simulate activity over time.

The session generator exposes an asynchronous `generate_session()` method. Delays are awaited rather than implemented with blocking sleeps, allowing other sessions to make progress while one session is waiting.

A simplified usage example:

```python
import asyncio

from src.generator.generator import EventGenerator
from src.generator.session import SessionGenerator
from src.producer.kafka_producer import KafkaEventProducer


async def main():
    producer = KafkaEventProducer(
        bootstrap_servers=(
            "localhost:9092,"
            "localhost:9093,"
            "localhost:9094"
        ),
        topic="ecommerce-events",
    )

    event_generator = EventGenerator()

    session_generator = SessionGenerator(
        event_generator=event_generator,
        producer=producer,
    )

    try:
        await session_generator.generate_session()
    finally:
        producer.flush(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
```

This is an illustrative usage pattern; adjust it to the actual constructor and publishing interfaces implemented in the repository.

### Concurrent session simulation

The generator is intended to simulate many concurrent users.

Because session generation includes delays, `asyncio` allows many session tasks to share a single event loop instead of requiring one operating-system thread and one event loop per session.

The intended concurrency model is:

```python
async def main():
    worker_count = 1000

    tasks = [
        asyncio.create_task(run_worker(worker_id))
        for worker_id in range(worker_count)
    ]

    await asyncio.gather(*tasks)
```

Each worker repeatedly generates sessions until a shutdown signal is received.

The exact worker implementation should be adapted to the current `SessionGenerator` and producer interfaces.

**Important:** `asyncio` improves resource efficiency when tasks spend time waiting, but it does not automatically increase throughput. Blocking operations and CPU-intensive work can still stall the event loop. Kafka publishing through `confluent-kafka` is asynchronous at the delivery level, but its `produce()` method is not itself an `async` coroutine.

### Kafka publishing

The `KafkaEventProducer` encapsulates publishing events to the `ecommerce-events` topic.

The intended producer configuration includes:

* `acks=all` to request acknowledgment from all in-sync replicas.
* Producer idempotence to help prevent duplicate writes caused by producer retries.
* Compression to reduce network traffic.
* A short batching delay to improve throughput.
* Delivery callbacks to observe message delivery results.

The producer should be shared by the session workers where appropriate, rather than creating a new Kafka producer for every event or session.

## Duplicate Events and Fraud Simulation

An important goal is to make the generated stream useful for testing data quality and fraud detection.

These features should be implemented without adding artificial labels that would reveal the answer to downstream consumers.

### Duplicate events

Duplicate simulation should publish the same logical event more than once, preserving its original `event_id` and payload.

For example:

```text
event_id: evt-123
event_type: payment_attempted
user_id: user-42
amount: 120.00
```

The duplicate should contain the same event identifier and data. This makes it possible to test deduplication logic in streaming and batch pipelines.

Duplicate events should be introduced deliberately by the application-level generator. Kafka producer idempotence serves a different purpose: it helps handle retry-related duplicates at the Kafka producer level.

### Suspicious activity patterns

Fraud simulation should generate ordinary events with suspicious combinations of attributes and behavior, rather than introducing a special `fraud_event` type or exposing an `is_fraudulent` field in the event payload.

Potential scenarios include:

* **High payment velocity:** Many payment attempts within a short time window.
* **Repeated payment failures:** Several failed attempts followed by another attempt or a successful payment.
* **Unusual transaction amount:** A transaction significantly larger than the user's typical activity.

A hidden ground-truth label may be retained separately for evaluating fraud-detection results, but it should not be included in the normal event stream consumed by the processing pipeline.

These scenarios are planned simulation capabilities unless they have already been implemented in the generator.

## Kafka Infrastructure

The local development environment uses a Kafka cluster running through Docker Compose.

The planned local cluster contains three Kafka nodes operating in KRaft mode, without a separate ZooKeeper service.

### Topic configuration

The primary topic is:

```text
ecommerce-events
```

A development configuration can use six partitions and a replication factor of three.

Example topic creation command from within the Docker network:

```bash
kafka-topics \
  --bootstrap-server kafka-1:19092 \
  --create \
  --topic ecommerce-events \
  --partitions 6 \
  --replication-factor 3
```

With three brokers, a replication factor of three places a replica on each broker. A minimum in-sync replica setting of two can improve durability while allowing the cluster to continue accepting writes when one replica is unavailable, provided the remaining replicas meet the producer's acknowledgment requirements.

### Bootstrap servers

When the generator runs directly on the host:

```text
localhost:9092,localhost:9093,localhost:9094
```

When it runs inside the Docker network:

```text
kafka-1:19092,kafka-2:19092,kafka-3:19092
```

Use the addresses appropriate to where the generator process runs. Docker-internal hostnames generally are not resolvable from the host machine.

The local environment uses plaintext Kafka communication for development and should not be treated as a production security configuration.

## Catalog Data

The generator can use a real product and merchant catalog to make synthetic activity more realistic.

The source data for payment generation is available at project root under *files* directory

```text
data/
├── products.csv
└── merchants.csv
```

The catalog provides realistic product and merchant attributes, while user behavior and event sequences remain synthetic.

Historical records should not simply be replayed as if they were newly generated activity. The goal is to generate new sessions and events using catalog entities as reference data.

