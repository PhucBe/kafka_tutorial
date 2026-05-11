import time
from datetime import datetime

import pandas as pd

from src.common.config import (
    BOOTSTRAP_SERVERS,
    ORDERS_CSV_PATH,
    PAYMENTS_CSV_PATH,
    TOPIC_ORDER_CREATED,
    TOPIC_PAYMENT_CONFIRMED,
    TOPIC_ORDER_DELIVERED,
    TOPIC_INVALID_EVENTS,
)
from src.common.kafka_utils import build_producer
from src.common.validators import validate_event


TOPIC_MAP = {
    "order_created": TOPIC_ORDER_CREATED,
    "payment_confirmed": TOPIC_PAYMENT_CONFIRMED,
    "order_delivered": TOPIC_ORDER_DELIVERED,
}


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = pd.read_csv(ORDERS_CSV_PATH)
    payments = pd.read_csv(PAYMENTS_CSV_PATH)
    return orders, payments


def build_order_created_events(orders: pd.DataFrame, limit: int = 20) -> list[dict]:
    events = []

    subset = (
        orders[["order_id", "customer_id", "order_purchase_timestamp"]]
        .dropna()
        .head(limit)
    )

    for _, row in subset.iterrows():
        events.append(
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "event_type": "order_created",
                "event_time": row["order_purchase_timestamp"],
            }
        )

    return events


def build_payment_confirmed_events(payments: pd.DataFrame, limit: int = 20) -> list[dict]:
    events = []

    subset = (
        payments[["order_id", "payment_sequential", "payment_value"]]
        .dropna()
        .head(limit)
    )

    for _, row in subset.iterrows():
        payment_sequential = int(row["payment_sequential"])

        events.append(
            {
                "order_id": row["order_id"],
                "payment_id": f'{row["order_id"]}_{payment_sequential}',
                "event_type": "payment_confirmed",
                "event_time": datetime.utcnow().isoformat(),
                "payment_value": float(row["payment_value"]),
            }
        )

    return events


def build_order_delivered_events(orders: pd.DataFrame, limit: int = 20) -> list[dict]:
    events = []

    subset = (
        orders[["order_id", "order_delivered_customer_date"]]
        .dropna()
        .head(limit)
    )

    for _, row in subset.iterrows():
        events.append(
            {
                "order_id": row["order_id"],
                "event_type": "order_delivered",
                "event_time": datetime.utcnow().isoformat(),
                "delivered_customer_date": row["order_delivered_customer_date"],
            }
        )

    return events


def main() -> None:
    producer = build_producer(BOOTSTRAP_SERVERS)

    orders, payments = load_data()

    all_events = (
        build_order_created_events(orders)
        + build_payment_confirmed_events(payments)
        + build_order_delivered_events(orders)
    )

    # Cố tình thêm event lỗi để test invalid/dead-letter topic
    invalid_test_events = [
        {
            "order_id": "BROKEN_001",
            "event_type": "payment_confirmed",
        },
        {
            "order_id": "BROKEN_002",
            "event_type": "order_created",
        },
        {
            "event_type": "payment_confirmed",
            "payment_value": 100.0,
        },
        {
            "order_id": "BROKEN_003",
            "event_type": "unknown_event",
        },
    ]

    all_events.extend(invalid_test_events)

    for event in all_events:
        is_valid, reason = validate_event(event)
        key = event.get("order_id", "unknown")

        if is_valid:
            topic = TOPIC_MAP[event["event_type"]]
        else:
            topic = TOPIC_INVALID_EVENTS
            event["invalid_reason"] = reason

        producer.send(topic, key=key, value=event)

        print(f"[PRODUCER] topic={topic} key={key} value={event}")

        time.sleep(0.2)

    producer.flush()
    producer.close()

    print("Historical replay producer finished.")


if __name__ == "__main__":
    main()