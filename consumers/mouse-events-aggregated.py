from confluent_kafka import Consumer
import json
import os
import time
from collections import defaultdict
from dotenv import load_dotenv
from shared.schemas import UserEvent



load_dotenv()
 
 
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
 
 
topic = os.getenv("KAFKA_TOPIC")
 
consumer.subscribe([topic])
 
 
# --- Windowed aggregation config ---
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "10")) # tracsks time in chunks defined by WINDOW_SECONDS
HEATMAP_GRID_SIZE = int(os.getenv("HEATMAP_GRID_SIZE", "50"))  # px per grid cell
 
session_event_counts = defaultdict(int)
heatmap_counts = defaultdict(int)
heatmap_counts_click = defaultdict(int)  # key: (grid_x, grid_y)
window_start = time.time()
 
 
def flush_window():
    global session_event_counts, heatmap_counts, window_start, heatmap_counts_click
 
    duration = time.time() - window_start
    print(f"\n=== Window flush ({duration:.1f}s) ===")
 
    if session_event_counts:
        print("Events per session:")
        for key, count in sorted(
            session_event_counts.items(), key=lambda kv: -kv[1]
        ):
            session_id, event_type = key

            if event_type == "mousemove":
                print(f"  {key}: {count} events")

        print("\n Clicks:")
        for key, count in sorted(
            session_event_counts.items(), key=lambda kv: -kv[1]
        ):
            session_id, event_type = key

            if event_type == "click":
                print(f"  {key}: {(count /  WINDOW_SECONDS):.2f}")
    else:
        print("No events this window.")
 
    if heatmap_counts:
        top_cells = sorted(
            heatmap_counts.items(), key=lambda kv: -kv[1]
        )[:5]
        print("\nTop heatmap cells (grid_x, grid_y): count")
        for (grid_x, grid_y), count in top_cells:
            print(f"  ({grid_x}, {grid_y}): {count}")

    if heatmap_counts_click:
        top_cells = sorted(
            heatmap_counts_click.items(), key=lambda kv: -kv[1]
        )[:5]
        print("\nTop click heatmap cells (grid_x, grid_y): count")
        for (grid_x, grid_y), count in top_cells:
            print(f"  ({grid_x}, {grid_y}): {count}")
 
    session_event_counts = defaultdict(int)
    heatmap_counts = defaultdict(int)
    heatmap_counts_click = defaultdict(int)
    window_start = time.time()
 
 
print(
    f"Consumer started. "
    f"Topic={topic} "
    f"Group={os.getenv('KAFKA_CONSUMER_GROUP')} "
    f"Window={WINDOW_SECONDS}s"
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
 
                session_event_counts[event.session_id, event.event_type] += 1
 
                grid_x = event.x // HEATMAP_GRID_SIZE
                grid_y = event.y // HEATMAP_GRID_SIZE
                heatmap_counts[(grid_x, grid_y)] += 1

                if event.event_type == "click":
                    heatmap_counts_click[(grid_x, grid_y)] += 1

#                print(
#                            f"Received event: "
#                            f"partition={message.partition()} "
#                            f"session={event.session_id} "
#                            f"x={event.x} "
#                            f"y={event.y}"
#                        )

        # Check the window on every poll cycle (~1s), not just on message
        # arrival, so idle periods still flush on schedule.
        if time.time() - window_start >= WINDOW_SECONDS:
            flush_window()
 
except KeyboardInterrupt:
    print("Stopping consumer...")
 
finally:
    flush_window()
    consumer.close()