import json
import os

from kafka import KafkaConsumer

from src.common.config import (
    BOOTSTRAP_SERVERS,
    RAW_EVENTS_OUTPUT_DIR,
    TOPIC_INVALID_EVENTS,
)


OUTPUT_PATH = os.path.join(RAW_EVENTS_OUTPUT_DIR, "invalid_events.jsonl")


def main() -> None:
    os.makedirs(RAW_EVENTS_OUTPUT_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC_INVALID_EVENTS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="invalid-writer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("invalid_events_consumer is listening...")

    with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
        for message in consumer:
            record = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "value": message.value,
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()

            print(f"[INVALID] {record}")


if __name__ == "__main__":
    main()