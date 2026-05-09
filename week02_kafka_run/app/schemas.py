REQUIRED_FIELDS_BY_EVENT_TYPE = {
    "order_created": [
        "order_id",
        "customer_id",
        "event_type",
        "event_time",
    ],
    "payment_confirmed": [
        "order_id",
        "payment_id",
        "event_type",
        "event_time",
        "payment_value",
    ],
    "order_delivered": [
        "order_id",
        "event_type",
        "event_time",
        "delivered_customer_date",
    ],
}


def validate_event(payload: dict) -> tuple[bool, str]:
    event_type = payload.get("event_type")

    if event_type not in REQUIRED_FIELDS_BY_EVENT_TYPE:
        return False, f"unsupported_event_type:{event_type}"
    
    required_fields = REQUIRED_FIELDS_BY_EVENT_TYPE[event_type]

    missing_fields = [
        field
        for field in required_fields
        if field not in payload or payload[field] in (None, "")
    ]

    if missing_fields:
        return False, f"missing_fields:{','.join(missing_fields)}"
    
    return True, "ok"