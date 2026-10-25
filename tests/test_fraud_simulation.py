import json
import unittest
from unittest.mock import AsyncMock, patch

from src.generator.generator import EventGenerator
from src.generator.session import SessionGenerator


class RecordingProducer:
    def __init__(self):
        self.events = []
        self.flushed = False

    def publish(self, event):
        self.events.append(event)

    def flush(self):
        self.flushed = True


class FraudSimulationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fraud_sessions_have_linked_attempts_and_outcomes(self):
        amounts = []
        for seed in range(12):
            producer = RecordingProducer()
            generator = SessionGenerator(EventGenerator(seed), producer, seed, fraud_rate=1)
            with patch('src.generator.session.asyncio.sleep', new_callable=AsyncMock):
                await generator.generate_session()
            events = list({event.event_id: event for event in producer.events}.values())
            self.assertEqual(len({(e.user_id, e.session_id, e.device_id) for e in events}), 1)
            attempts = [e for e in events if e.event_type == 'payment_attempted']
            outcomes = [e for e in events if e.event_type in ('payment_failed', 'payment_completed')]
            self.assertTrue(6 <= len(attempts) <= 12)
            self.assertEqual(len(attempts), len(outcomes))
            self.assertTrue(all(e.event_type == 'payment_failed' for e in outcomes[:-1]))
            for attempt, outcome in zip(attempts, outcomes):
                self.assertEqual((attempt.amount, attempt.payment_method), (outcome.amount, outcome.payment_method))
            for event in events:
                self.assertNotIn('is_fraudulent', json.loads(event.to_json()))
            self.assertTrue(producer.flushed)
            amounts.append(attempts[0].amount)
        self.assertTrue(any(1 <= amount <= 5 for amount in amounts))
        self.assertTrue(any(amount > 5 for amount in amounts))

    def test_fraud_rate_bounds(self):
        for rate in (-0.1, 1.1, float('nan')):
            with self.assertRaises(ValueError):
                SessionGenerator(EventGenerator(), RecordingProducer(), fraud_rate=rate)
        generator = SessionGenerator(EventGenerator(), RecordingProducer(), fraud_rate=0)
        self.assertFalse(any(generator._create_session().is_fraudulent for _ in range(100)))

    def test_event_overrides_and_default_login(self):
        generator = EventGenerator(1)
        self.assertIsNotNone(generator.user_logged_in().session_id)
        context = dict(user_id=1, session_id='session', device_id='device')
        payment = generator.payment_attempted(**context, amount=2, payment_method='card')
        failure = generator.payment_failed(**context, amount=2, payment_method=payment.payment_method,
                                           failure_reason='fraud_suspected')
        self.assertEqual(failure.failure_reason, 'fraud_suspected')
