from confluent_kafka import Consumer, Producer
import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv
from shared.schemas import UserEvent, SessionAggregateEvent, HeatmapAggregateEvent


load_dotenv()


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC"
)

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP"
)

KAFKA_AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest",
)

AGGREGATE_TOPIC = os.getenv(
    "KAFKA_AGGREGATE_TOPIC",
    "mouse-events-aggregated",
)

WINDOW_SECONDS = int(
    os.getenv("WINDOW_SECONDS", "10")
)

HEATMAP_GRID_SIZE = int(
    os.getenv("HEATMAP_GRID_SIZE", "50")
)


# ============================================================
# Kafka consumer
# ============================================================

consumer = Consumer({
    "bootstrap.servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS"
    ),
    "group.id": os.getenv(
        "KAFKA_CONSUMER_GROUP"
    ),
    "auto.offset.reset": os.getenv(
        "KAFKA_AUTO_OFFSET_RESET"
    ) or "earliest"
})
consumer.subscribe([KAFKA_TOPIC])



# ============================================================
# Kafka producer for aggregate events (this consumer is also a producer for the aggregated topic)
# ============================================================


aggregate_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 10,
    "batch.size": 16384,
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Aggregate delivery failed: {err}")


def publish_aggregate(record: SessionAggregateEvent | HeatmapAggregateEvent, key: str):
    aggregate_producer.produce(
        AGGREGATE_TOPIC,
        key=key,
        value=json.dumps(record.model_dump(), default=str).encode("utf-8"),
        callback=delivery_report,
    )
    aggregate_producer.poll(0)


# ============================================================
# Window state
# ============================================================

session_event_counts = defaultdict(int)
heatmap_counts = defaultdict(int)
window_start = time.time()

# ============================================================
# Session aggregation
# ============================================================

def process_session_aggregates( window_start_iso: str,
                                window_end_iso: str,
                                duration: float,
                                ):
    for (session_id, event_type), count in session_event_counts.items():

        if event_type == "mousemove":
            print(
                f"  session={session_id}: "
                f"{count} mousemoves"
            )
        elif event_type == "click":
            print(
                f"  session={session_id}: "
                f"{count} clicks "
                f"({count / duration:.2f}/s)"
            )
            aggregate = SessionAggregateEvent(
                metric="session_event_count",
                window_start=window_start_iso,
                window_end=window_end_iso,
                session_id=session_id,
                event_type=event_type,
                count=count,
                rate_per_second=(
                    round(count / duration, 4)
                    if duration else 0
                ),
            )

            publish_aggregate(
                aggregate,
                key=session_id,
            )



# ============================================================
# Heatmap aggregation
# ============================================================

def process_heatmap_aggregates( 
                                counts,                                
                                window_start_iso,
                                window_end_iso,
                            ):
    for (session_id, event_type, grid_x, grid_y, element), count in counts.items():
        aggregate = HeatmapAggregateEvent(
            session_id=session_id,
            metric="heatmap_cell",            
            window_start=window_start_iso,
            window_end=window_end_iso,
            event_type=event_type,
            grid_x=grid_x,
            grid_y=grid_y,
            element=element,
            count=count,
        )

        publish_aggregate(
            aggregate,
            key=session_id,
        )




# ============================================================
# Window flush
# ============================================================


def flush_window():
    global session_event_counts
    global heatmap_counts
    global window_start

    window_end = time.time()

    duration = window_end - window_start

    window_start_iso = datetime.fromtimestamp(
        window_start,
        tz=timezone.utc,
    ).isoformat()

    window_end_iso = datetime.fromtimestamp(
        window_end,
        tz=timezone.utc,
    ).isoformat()

    print(
        f"\n=== Window flush ({duration:.1f}s) ==="
    )

    process_session_aggregates(
        window_start_iso,
        window_end_iso,
        duration,
    )

    process_heatmap_aggregates(
        heatmap_counts,
        window_start_iso,
        window_end_iso,
    )

    # Reset the state for the next window.
    session_event_counts = defaultdict(int)
    heatmap_counts = defaultdict(int)

    window_start = time.time()


# ============================================================
# Consumer loop
# ============================================================

print(
    f"Consumer started. "
    f"Topic={KAFKA_TOPIC} "
    f"Group={KAFKA_CONSUMER_GROUP} "
    f"Window={WINDOW_SECONDS}s "
    f"Grid={HEATMAP_GRID_SIZE}px "
    f"AggregateTopic={AGGREGATE_TOPIC}"
)


try:
    while True:
        message = consumer.poll(1.0)

        if message is not None:
            if message.error():
                print(f"Kafka error: {message.error()}")
            else:
                data = json.loads(
                    message.value().decode("utf-8")
                )
                event = UserEvent(**data)

                session_event_counts[(event.session_id, event.event_type)] += 1

                grid_x = event.x // HEATMAP_GRID_SIZE
                grid_y = event.y // HEATMAP_GRID_SIZE
                
                heatmap_counts[(event.session_id, event.event_type, grid_x, grid_y, event.element)] += 1

        # Check the window on every poll cycle (1s)
        if time.time() - window_start >= WINDOW_SECONDS:
            flush_window()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    flush_window()
    aggregate_producer.flush()
    consumer.close()