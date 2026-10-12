import random
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.generator.generator import Event, EventGenerator


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
        seed: int | None = None,
    ):
        self.event_generator = event_generator
        self.random = random.Random(seed)

    def _create_session(self) -> Session:
        user_id = self.random.randint(1, 100_000)

        return Session(
            user_id=user_id,
            session_id=str(uuid.uuid4()),
            device_id=str(uuid.uuid4()),
            is_fraudulent=self.random.random() < self.FRAUD_RATE,
        )

    def generate_session(self) -> list[Event]:
        session = self._create_session()

        events: list[Event] = [self.event_generator.user_logged_in()]

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

            events.append(
                self.event_generator.product_viewed(
                    user_id=session.user_id,
                    session_id=session.session_id,
                    device_id=session.device_id,
                    product_id=product_id,
                    merchant_id=merchant_id,
                )
            )

            # ---------------------------------------------------------
            # 2. User decides whether to add something to cart
            # ---------------------------------------------------------

            # 35% of browsing sessions add a product to cart.
            if self.random.random() > 0.1:
                continue

            amount += product.iloc[0, 4]

            events.append(
                self.event_generator.product_added_to_cart(
                    user_id=session.user_id,
                    session_id=session.session_id,
                    device_id=session.device_id,
                    product_id=product_id,
                    merchant_id=merchant_id,
                )
            )

        # ---------------------------------------------------------
        # 3. Checkout
        # ---------------------------------------------------------

        # Some users abandon their cart.
        if self.random.random() > 0.4 or amount == 0:
            return events

        events.append(
            self.event_generator.checkout_started(
                user_id=session.user_id,
                session_id=session.session_id,
                device_id=session.device_id,
            )
        )

        # ---------------------------------------------------------
        # 4. Payment
        # ---------------------------------------------------------

        payment = self.event_generator.payment_attempted(
            user_id=session.user_id,
            session_id=session.session_id,
            device_id=session.device_id,
            amount=round(amount, 2),
        )

        events.append(payment)

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

            events.append(completed)

            # Small percentage of successful transactions get refunded.
            if self.random.random() < 0.05:
                refund_amount = round(
                    payment.amount
                    * self.random.uniform(0.25, 1.0),
                    2,
                )

                events.append(
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

            events.append(failed)

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

                events.append(retry)

                # 70% of retries succeed.
                if self.random.random() < 0.70:
                    events.append(
                        self.event_generator.payment_completed(
                            user_id=session.user_id,
                            session_id=session.session_id,
                            device_id=session.device_id,
                            amount=retry.amount,
                            payment_method=retry.payment_method,
                        )
                    )

        # Generate random duplicates
        events = self.__generate_duplicate(events)

        return events

    def __generate_duplicate(
            self,
            events: list[Event],
            probability: float = 0.02,
    ) -> list[Event]:

        result = []

        for event in events:
            result.append(event)

            if self.random.random() < probability:
                result.append(event)

        return result