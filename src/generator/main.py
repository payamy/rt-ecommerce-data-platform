from src.generator.generator import EventGenerator
from src.generator.session import SessionGenerator


event_generator = EventGenerator(seed=42)

session_generator = SessionGenerator(
    event_generator=event_generator,
    # seed=42,
)

events = session_generator.generate_session()

for event in events:
    print(event.to_json())
