import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum


class EventType(StrEnum):
    USER_REGISTERED = "user_registered"
    PRODUCT_VIEWED = "product_viewed"
    PRODUCT_ADDED_TO_CART = "product_added_to_cart"
    CHECKOUT_STARTED = "checkout_started"
    PAYMENT_ATTEMPTED = "payment_attempted"
    PAYMENT_COMPLETED = "payment_completed"
    PAYMENT_FAILED = "payment_failed"
    REFUND_CREATED = "refund_created"


@dataclass
class Event:
    event_id: str
    event_type: str
    event_time: str

    user_id: int
    session_id: str
    device_id: str

    product_id: int | None = None
    merchant_id: int | None = None

    amount: float | None = None

    payment_method: str | None = None
    failure_reason: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))


class EventGenerator:

    PAYMENT_METHODS = [
        "card",
        "paypal",
        "bank_transfer",
        "wallet",
    ]

    FAILURE_REASONS = [
        "insufficient_funds",
        "card_declined",
        "expired_card",
        "fraud_suspected",
        "processor_error",
    ]

    def __init__(self, seed: int | None = None):
        self.random = random.Random(seed)

    @staticmethod
    def _base_event(
        event_type: EventType,
        user_id: int,
        session_id: str,
        device_id: str,
    ) -> Event:
        return Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            event_time=datetime.now(timezone.utc).isoformat(),
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

    def user_logged_in(self) -> Event:
        user_id = self.random.randint(1, 100_000)
        session_id = str(uuid.uuid4())
        device_id = str(uuid.uuid4())

        return self._base_event(
            event_type=EventType.USER_REGISTERED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

    def product_viewed(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        product_id: int,
        merchant_id: int,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.PRODUCT_VIEWED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        event.product_id = product_id
        event.merchant_id = merchant_id

        return event

    def product_added_to_cart(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        product_id: int,
        merchant_id: int,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.PRODUCT_ADDED_TO_CART,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        event.product_id = product_id
        event.merchant_id = merchant_id

        return event

    def checkout_started(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.CHECKOUT_STARTED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        return event

    def payment_attempted(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        amount: float,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.PAYMENT_ATTEMPTED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        event.amount = amount
        event.payment_method = self.random.choice(
            self.PAYMENT_METHODS
        )

        return event

    def payment_completed(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        amount: float,
        payment_method: str,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.PAYMENT_COMPLETED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )
        event.amount = amount
        event.payment_method = payment_method

        return event

    def payment_failed(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        amount: float,
        payment_method: str,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.PAYMENT_FAILED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        event.amount = amount
        event.payment_method = payment_method
        event.failure_reason = self.random.choice(
            self.FAILURE_REASONS
        )

        return event

    def refund_created(
        self,
        user_id: int,
        session_id: str,
        device_id: str,
        amount: float,
    ) -> Event:
        event = self._base_event(
            event_type=EventType.REFUND_CREATED,
            user_id=user_id,
            session_id=session_id,
            device_id=device_id,
        )

        event.amount = amount

        return event