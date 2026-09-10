from confluent_kafka import Consumer
import json
import os
from dotenv import load_dotenv
from database.database import SessionLocal, Base, configure_database
from database.models import SessionClickRate, HeatmapCell
from shared.schemas import SessionAggregateEvent, HeatmapAggregateEvent
 
# Load the variables from the .env file
load_dotenv()


# ============================================================
# Configuration
# ============================================================
 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_AGGREGATE_TOPIC", "mouse-events-aggregated")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "aggregate-writer")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")



# ============================================================
# Table setup
# ============================================================
 
def setup_tables(force_refresh: bool = False):
    """Creates all tables registered on Base if they don't already exist.
    If force_refresh is True, drops and recreates them (useful in dev)."""
    engine = configure_database()
    if force_refresh:
        print("Dropping and recreating tables...")
        Base.metadata.drop_all(bind=engine)
 
    Base.metadata.create_all(bind=engine)
    print("Tables verified/created.")
 
 
# ============================================================
# Insert helpers
# ============================================================
 
def insert_session_aggregate(event: SessionAggregateEvent):
    row = SessionClickRate(
        session_id=event.session_id,
        window_start=event.window_start,
        window_end=event.window_end,
        event_type=event.event_type,
        count=event.count,
        rate_per_second=event.rate_per_second,
    )
    with SessionLocal() as session:
        session.add(row)
        session.commit()
 
 
def insert_heatmap_aggregate(event: HeatmapAggregateEvent):
    row = HeatmapCell(
        session_id=event.session_id,
        window_start=event.window_start,
        window_end=event.window_end,
        event_type=event.event_type,
        grid_x=event.grid_x,
        grid_y=event.grid_y,
        element=event.element,
        count=event.count,
    )
    with SessionLocal() as session:
        session.add(row)
        session.commit()
 
 
def handle_message(data: dict):
    metric = data.get("metric")
 
    if metric == "session_event_count":
        insert_session_aggregate(SessionAggregateEvent(**data))
 
    elif metric == "heatmap_cell":
        insert_heatmap_aggregate(HeatmapAggregateEvent(**data))
 
    else:
        print(f"Unknown metric, skipping: {metric}")

# ============================================================
# Kafka consumer
# ============================================================
 
#consumer = Consumer({
#    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
#    "group.id": KAFKA_CONSUMER_GROUP,
#    "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
#})
#consumer.subscribe([KAFKA_TOPIC])

def create_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
    })

    consumer.subscribe([KAFKA_TOPIC])

    return consumer



def main():
    consumer = create_consumer()

    setup_tables(force_refresh=False)

    print(
        f"Aggregate writer started. "
        f"Topic={KAFKA_TOPIC} "
        f"Group={KAFKA_CONSUMER_GROUP}"
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            try:
                data = json.loads(
                    message.value().decode("utf-8")
                )

                handle_message(data)

            except Exception as exc:
                print(f"Failed to process message: {exc}")

    except KeyboardInterrupt:
        print("Stopping aggregate writer...")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()