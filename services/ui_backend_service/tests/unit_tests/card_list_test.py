import importlib.util
from pathlib import Path

_CARD_INFO = Path(__file__).resolve().parents[2] / "api" / "card_info.py"
_SPEC = importlib.util.spec_from_file_location("card_info", _CARD_INFO)
card_info = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(card_info)


def test_cards_from_card_info_records_uses_field_name_and_payload_type():
    cards = card_info.cards_from_card_info_records(
        [
            {
                "field_name": "3067b7280c294132af6205034e2f816f",
                "type": "card-info",
                "value": (
                    '{"card_uuid": "3067b7280c294132af6205034e2f816f",'
                    ' "rank": null, "type": "blank", "id": null}'
                ),
            }
        ]
    )
    assert cards == {
        "3067b7280c294132af6205034e2f816f": {"id": None, "type": "blank"}
    }


def test_cards_from_card_info_records_skips_rows_without_hash():
    assert card_info.cards_from_card_info_records([{"field_name": "", "value": "{}"}]) == {}
    assert card_info.cards_from_card_info_records(None) == {}
