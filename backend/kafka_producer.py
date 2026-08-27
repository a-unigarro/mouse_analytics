import os

from confluent_kafka import Producer
import json
from dotenv import load_dotenv
from shared.schemas import UserEvent

load_dotenv()
producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "linger.ms": 10,
    "batch.size": 16384,
})


def delivery_report(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to "
            f"{msg.topic()} "
            f"[{msg.partition()}] "
            f"offset {msg.offset()}"
        )


def send_event(event: UserEvent):

    producer.produce(
        "mouse-events",
        key=event.session_id,
        value=json.dumps(
            event.model_dump(),
            default=str
        ).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0) ## if there are any (finished) events in the queue, this will trigger the delivery report callback for them


def shutdown_producer():

    producer.flush()