import asyncio
import random
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.generator.generator import Event, EventGenerator
from src.producer.kafka_producer import KafkaEventProducer


@dataclass
class Session:
    user_id: int
    session_id: str
    device_id: str
    is_fraudulent: bool


class SessionGenerator:
    """
    Generates realistic e-commerce user sessions.

    A session follows a probabilistic state machine rather than
    generating completely independent events.
    """

    FRAUD_RATE = 0.02


    def __init__(
        self,
        event_generator: EventGenerator,
        producer: KafkaEventProducer,
        seed: int | None = None,
    ):
        self.event_generator = event_generator
        self.random = random.Random(seed)
        self.producer = producer

    def _create_session(self) -> Session:
        user_id = self.random.randint(1, 100_000)

        return Session(
            user_id=user_id,
            session_id=str(uuid.uuid4()),
            device_id=str(uuid.uuid4()),
            is_fraudulent=self.random.random() < self.FRAUD_RATE,
        )

    async def generate_session(self) -> None:
        session = self._create_session()

        await self.__send_event(self.event_generator.user_logged_in())
        # A delay after logging in
        await asyncio.sleep(self.random.randint(1, 10))

        # Number of products the user views.
        view_count = self.random.randint(1, 6)

        # ---------------------------------------------------------
        # 1. Product browsing
        # ---------------------------------------------------------

        products_file = Path(__file__).parent.parent.parent.joinpath("files", "products.csv")
        products = pd.read_csv(products_file)
        amount = 0

        for _ in range(view_count):

            product = products.sample(n=1)
            product_id = product.iloc[0, 0]
            merchant_id = product.iloc[0, 3]

            await self.__send_event(
                self.event_generator.product_viewed(
                    user_id=session.user_id,
                    session_id=session.session_id,
                    device_id=session.device_id,
                    product_id=product_id,
                    merchant_id=merchant_id,
                )
            )
            # A delay to see if the product is what customer wants
            await asyncio.sleep(self.random.randint(1, 3))

            # ---------------------------------------------------------
            # 2. User decides whether to add something to cart
            # ---------------------------------------------------------

            # 35% of browsing sessions add a product to cart.
            if self.random.random() > 0.1:
                continue

            amount += product.iloc[0, 4]

            await self.__send_event(
                self.event_generator.product_added_to_cart(
                    user_id=session.user_id,
                    session_id=session.session_id,
                    device_id=session.device_id,
                    product_id=product_id,
                    merchant_id=merchant_id,
                )
            )
            # A delay after adding item to cart
            await asyncio.sleep(self.random.randint(3, 6))

        # ---------------------------------------------------------
        # 3. Checkout
        # ---------------------------------------------------------

        # Some users abandon their cart.
        if self.random.random() > 0.4 or amount == 0:
            return


        await self.__send_event(
            self.event_generator.checkout_started(
                user_id=session.user_id,
                session_id=session.session_id,
                device_id=session.device_id,
            )
        )
        # A delay after adding item to cart
        await asyncio.sleep(self.random.randint(3, 6))

        # ---------------------------------------------------------
        # 4. Payment
        # ---------------------------------------------------------

        payment = self.event_generator.payment_attempted(
            user_id=session.user_id,
            session_id=session.session_id,
            device_id=session.device_id,
            amount=round(amount, 2),
        )

        await self.__send_event(payment)
        # A delay after attempting payment
        await asyncio.sleep(self.random.randint(5, 10))

        # ---------------------------------------------------------
        # 5. Payment outcome
        # ---------------------------------------------------------

        # 85% successful payment.
        if self.random.random() < 0.85:
            completed = self.event_generator.payment_completed(
                user_id=session.user_id,
                session_id=session.session_id,
                device_id=session.device_id,
                amount=payment.amount,
                payment_method=payment.payment_method,
            )

            await self.__send_event(completed)

            # Small percentage of successful transactions get refunded.
            if self.random.random() < 0.05:
                refund_amount = round(
                    payment.amount
                    * self.random.uniform(0.25, 1.0),
                    2,
                )
                # A delay after before refund operation
                await asyncio.sleep(self.random.randint(3, 6))

                await self.__send_event(
                    self.event_generator.refund_created(
                        user_id=session.user_id,
                        session_id=session.session_id,
                        device_id=session.device_id,
                        amount=refund_amount,
                    )
                )

        else:
            failed = self.event_generator.payment_failed(
                user_id=session.user_id,
                session_id=session.session_id,
                device_id=session.device_id,
                amount=payment.amount,
                payment_method=payment.payment_method,
            )

            await self.__send_event(failed)

            # 40% of failed payments are retried.
            if self.random.random() < 0.40:
                retry = self.event_generator.payment_attempted(
                    user_id=session.user_id,
                    session_id=session.session_id,
                    device_id=session.device_id,
                    amount=amount
                )

                # Keep the same merchant/payment characteristics
                # for a retry.
                retry.amount = payment.amount
                retry.merchant_id = payment.merchant_id
                retry.payment_method = payment.payment_method

                await self.__send_event(retry)

                # 70% of retries succeed.
                if self.random.random() < 0.70:
                    # A delay after before retry
                    await asyncio.sleep(self.random.randint(3, 6))
                    await self.__send_event(
                        self.event_generator.payment_completed(
                            user_id=session.user_id,
                            session_id=session.session_id,
                            device_id=session.device_id,
                            amount=retry.amount,
                            payment_method=retry.payment_method,
                        )
                    )

        self.producer.flush()


    async def __send_event(self, event: Event, probability: float = 0.02) -> None:
        self.producer.publish(event)
        if self.random.random() < probability:
            await asyncio.sleep(self.random.randint(5, 10))
            self.producer.publish(event)
