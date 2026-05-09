import json
import os

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "orders_events"
GROUP_ID = "metrics-group-manual-demo"


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(
        f"[MANUAL-COMMIT] listening "
        f"topic={TOPIC_NAME}, "
        f"group_id={GROUP_ID}"
    )

    for message in consumer:
        print(
            f"[MANUAL-COMMIT] processing "
            f"partition={message.partition}, "
            f"offset={message.offset}, "
            f"key={message.key}, "
            f"value={message.value}"
        )

        consumer.commit()

        print(
            f"[MANUAL-COMMIT] committed "
            f"partition={message.partition}, "
            f"offset={message.offset}"
        )


if __name__ == "__main__":
    main()