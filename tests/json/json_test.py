import pytest
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from dubbo.codec.json_codec.json_codec_handler import JsonTransportCodec

def test_json_single_parameter_roundtrip():
    codec = JsonTransportCodec(parameter_types=[int], return_type=int)

    # Encode a single int
    encoded = codec.encode_parameters(42)
    assert isinstance(encoded, bytes)

    # Decode back
    decoded = codec.decode_return_value(encoded)
    assert decoded == 42


def test_json_multiple_parameters_roundtrip():
    codec = JsonTransportCodec(parameter_types=[str, int], return_type=str)

    # Encode multiple args
    encoded = codec.encode_parameters("hello", 123)
    assert isinstance(encoded, bytes)

    # Decode return (simulate server returning str)
    return_encoded = codec.get_encoder().encode(("world",))
    decoded = codec.decode_return_value(return_encoded)
    assert decoded == "world"


def test_json_complex_types():
    codec = JsonTransportCodec(parameter_types=[dict], return_type=dict)

    obj = {
        "name": "Alice",
        "when": datetime(2025, 8, 27, 12, 30),
        "price": Decimal("19.99"),
        "ids": {uuid4(), uuid4()}
    }

    encoded = codec.encode_parameters(obj)
    assert isinstance(encoded, bytes)

    decoded = codec.decode_return_value(encoded)
    assert isinstance(decoded, dict)
    assert decoded["name"] == "Alice"
    assert isinstance(decoded["price"], Decimal)
    assert isinstance(decoded["when"], datetime)
    assert isinstance(decoded["ids"], set)

