import json
import os
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

from src.common.config import (
    BOOTSTRAP_SERVERS,
    METRICS_OUTPUT_DIR,
    TOPIC_ORDER_CREATED,
)


OUTPUT_PATH = os.path.join(METRICS_OUTPUT_DIR, "order_created_per_minute.csv")


def normalize_to_minute(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", ""))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "unknown"


def write_metrics_to_csv(metrics: dict[str, int]) -> None:
    os.makedirs(METRICS_OUTPUT_DIR, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("minute_bucket,order_created_count\n")

        for minute_bucket, count in sorted(metrics.items()):
            f.write(f"{minute_bucket},{count}\n")


def main() -> None:
    metrics = defaultdict(int)

    consumer = KafkaConsumer(
        TOPIC_ORDER_CREATED,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="metrics-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("metrics_consumer is listening...")

    for message in consumer:
        payload = message.value

        if payload.get("event_type") == "order_created":
            minute_bucket = normalize_to_minute(payload.get("event_time", ""))

            metrics[minute_bucket] += 1
            write_metrics_to_csv(metrics)

            print(
                f"[METRICS] topic={message.topic} "
                f"partition={message.partition} "
                f"offset={message.offset} "
                f"minute={minute_bucket} "
                f"count={metrics[minute_bucket]}"
            )


if __name__ == "__main__":
    main()