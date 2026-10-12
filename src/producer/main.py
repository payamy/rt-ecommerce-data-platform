import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.generator.generator import EventGenerator
from src.generator.session import SessionGenerator
from src.producer.kafka_producer import KafkaEventProducer


def run_worker(
    worker_id: int,
    producer: KafkaEventProducer,
    stop_event: threading.Event,
) -> None:
    # Each worker has its own generator and random state.
    event_generator = EventGenerator(seed=worker_id * 1000)
    session_generator = SessionGenerator(
        event_generator=event_generator,
        # seed=worker_id * 1000 + 1,
    )

    rng = random.Random(worker_id)

    while not stop_event.is_set():
        events = session_generator.generate_session()

        for event in events:
            if stop_event.is_set():
                break

            producer.publish(event)

        # Small randomized delay to simulate concurrent user activity.
        stop_event.wait(rng.uniform(0.05, 0.2))


def main() -> None:
    worker_count = 5
    stop_event = threading.Event()

    producer = KafkaEventProducer(
        bootstrap_servers="localhost:9092",
        topic="ecommerce-events",
    )

    executor = ThreadPoolExecutor(max_workers=worker_count)

    futures = [
        executor.submit(run_worker, worker_id, producer, stop_event)
        for worker_id in range(worker_count)
    ]

    try:
        while True:
            # Surface unexpected worker failures.
            for future in futures:
                if future.done():
                    future.result()

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Stopping session workers...")

    finally:
        stop_event.set()
        executor.shutdown(wait=True)
        producer.flush()


if __name__ == "__main__":
    main()