from confluent_kafka import Consumer
import json
import os 
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


print(
    f"Consumer started. "
    f"Topic={topic} "
    f"Group={os.getenv('KAFKA_CONSUMER_GROUP')}"
)



try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}")
            continue

        data = json.loads(
            message.value().decode("utf-8")
        )

        event = UserEvent(**data)
#        print(event)
        print(
            f"Received event: "
            f"partition={message.partition()} "
            f"session={event.session_id} "
            f"x={event.x} "
            f"y={event.y}"
        )

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()