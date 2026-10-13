from confluent_kafka import Producer

from src.generator.generator import Event


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "ecommerce-event-generator",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 10,
        })

    @staticmethod
    def _on_delivery(error, message):
        if error is not None:
            print(f"Delivery failed: {error}")
        else:
            print(
                f"Delivered to {message.topic()} "
                f"[{message.partition()}] "
                f"offset={message.offset()} "
                f"key={message.key()} "
            )

    def publish(self, event: Event) -> None:
        self.producer.produce(
            topic=self.topic,
            key=str(event.user_id),
            value=event.to_json(),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)

    def flush(self) -> None:
        remaining = self.producer.flush(timeout=10)

        if remaining:
            raise TimeoutError(
                f"{remaining} messages were not delivered before timeout"
            )