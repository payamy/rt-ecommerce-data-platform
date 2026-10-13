import asyncio
import random

from src.generator.generator import EventGenerator
from src.generator.session import SessionGenerator
from src.producer.kafka_producer import KafkaEventProducer


async def run_worker(
    worker_id: int,
    producer: KafkaEventProducer,
    stop_event: asyncio.Event,
) -> None:
    session_generator = SessionGenerator(
        event_generator=EventGenerator(),
        producer=producer,
    )
    rng = random.Random(worker_id)

    while not stop_event.is_set():
        await session_generator.generate_session()
        await asyncio.sleep(rng.uniform(0.05, 0.2))


async def main_async() -> None:
    worker_count = 1000
    stop_event = asyncio.Event()

    producer = KafkaEventProducer(
        bootstrap_servers="localhost:9092,localhost:9093,localhost:9094",
        topic="ecommerce-events",
    )

    tasks = [
        asyncio.create_task(
            run_worker(worker_id, producer, stop_event)
        )
        for worker_id in range(worker_count)
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        stop_event.set()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("Session workers stopped.")
