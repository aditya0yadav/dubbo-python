#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from pathlib import Path
from uuid import UUID
from decimal import Decimal
from datetime import datetime, date, time
from dataclasses import dataclass
from enum import Enum
from pydantic import BaseModel
from dubbo.codec.json_codec import JsonTransportCodec


# Optional dataclass and enum examples
@dataclass
class SampleDataClass:
    field1: int
    field2: str


class Color(Enum):
    RED = "red"
    GREEN = "green"


class SamplePydanticModel(BaseModel):
    name: str
    value: int


# List of test cases: (input_value, expected_type_after_decoding)
test_cases = [
    ("simple string", str),
    (12345, int),
    (12.34, float),
    (True, bool),
    (datetime(2025, 8, 27, 13, 0, 0), datetime),
    (date(2025, 8, 27), date),
    (time(13, 0, 0), time),
    (Decimal("123.45"), Decimal),
    (set([1, 2, 3]), set),
    (frozenset(["a", "b"]), frozenset),
    (UUID("12345678-1234-5678-1234-567812345678"), UUID),
    (Path("/tmp/file.txt"), Path),
    (Color.RED, Color),
    (SamplePydanticModel(name="test", value=42), SamplePydanticModel),
]


@pytest.mark.parametrize("value,expected_type", test_cases)
def test_json_codec_roundtrip(value, expected_type):
    codec = JsonTransportCodec(parameter_types=[type(value)], return_type=type(value))

    # Encode
    encoded = codec.encode_parameters(value)
    assert isinstance(encoded, bytes)

    # Decode
    decoded = codec.decode_return_value(encoded)

    # For pydantic models, compare dict representation
    if hasattr(value, "dict") and callable(value.dict):
        assert decoded.dict() == value.dict()
    # For dataclass, compare asdict
    elif hasattr(value, "__dataclass_fields__"):
        from dataclasses import asdict

        assert asdict(decoded) == asdict(value)
    # For sets/frozensets, compare as sets
    elif isinstance(value, (set, frozenset)):
        assert decoded == value
    # For enum
    elif isinstance(value, Enum):
        assert decoded.value == value.value
    else:
        assert decoded == value
