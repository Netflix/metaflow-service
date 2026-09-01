import json


def cards_from_card_info_records(records):
    """Turn task `card-info` metadata rows into the /cards list payload."""
    cards = {}
    for record in records or []:
        raw_value = record.get("value") or "{}"
        if isinstance(raw_value, dict):
            payload = raw_value
        else:
            try:
                payload = json.loads(raw_value)
            except (TypeError, json.JSONDecodeError):
                payload = {}
        card_hash = record.get("field_name") or payload.get("card_uuid")
        if not card_hash:
            continue
        cards[str(card_hash)] = {
            "id": payload.get("id"),
            "type": payload.get("type") or "blank",
        }
    return cards
