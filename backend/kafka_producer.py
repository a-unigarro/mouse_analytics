from confluent_kafka import Producer
import json

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


def send_event(event):
    producer.produce(
        "mouse-events",
        value=json.dumps(
            event.model_dump(),
            default=str
        ).encode("utf-8")
    )

    producer.flush()