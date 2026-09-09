import json
import time
import os
from confluent_kafka import Consumer, Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9093",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "mouse-events-test",
)


def test_kafka_produce_and_consume():

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "kafka-integration-test",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([KAFKA_TOPIC])

    message = {
        "event_type": "mousemove",
        "session_id": "integration-test-session",
        "x": 100,
        "y": 200,
    }

    producer.produce(
        KAFKA_TOPIC,
        value=json.dumps(message).encode("utf-8"),
    )

    producer.flush()

    received_message = None

    deadline = time.time() + 10

    while time.time() < deadline:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise RuntimeError(msg.error())

        received_message = json.loads(
            msg.value().decode("utf-8")
        )

        break

    consumer.close()

    assert received_message == message