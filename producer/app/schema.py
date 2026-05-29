from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastavro import parse_schema, validation


SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "warehouse_event_v2.avsc"
PARSED_SCHEMA = parse_schema(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def validate_against_schema(payload: dict[str, Any]) -> None:
    if not validation.validate(payload, PARSED_SCHEMA):
        raise ValueError("payload does not satisfy Avro schema")
