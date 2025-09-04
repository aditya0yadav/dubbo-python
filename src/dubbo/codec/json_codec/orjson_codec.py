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

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from dubbo.codec.json_codec import JsonCodec

__all__ = ["OrJsonCodec"]


class OrJsonCodec(JsonCodec):
    """
    orjson codec implementation for high-performance JSON encoding/decoding.

    Uses the orjson library if available, otherwise falls back gracefully.
    """

    def __init__(self):
        try:
            import orjson

            self.orjson = orjson
            self.available = True
        except ImportError:
            self.available = False

    def encode(self, obj: Any) -> bytes:
        """
        Encode an object to JSON bytes using orjson.

        :param obj: The object to encode.
        :type obj: Any
        :return: The encoded JSON bytes.
        :rtype: bytes
        """
        if not self.available:
            raise ImportError("orjson not available")
        return self.orjson.dumps(obj, default=self._default_handler)

    def decode(self, data: bytes) -> Any:
        """
        Decode JSON bytes to an object using orjson.

        :param data: The JSON bytes to decode.
        :type data: bytes
        :return: The decoded object.
        :rtype: Any
        """
        if not self.available:
            raise ImportError("orjson not available")
        return self.orjson.loads(data)

    def can_handle(self, obj: Any) -> bool:
        """
        Check if this codec can handle the given object.

        :param obj: The object to check.
        :type obj: Any
        :return: True if orjson is available.
        :rtype: bool
        """
        return self.available

    def _default_handler(self, obj):
        """
        Handle types not supported natively by orjson.

        :param obj: The object to serialize.
        :return: Serialized representation.
        """
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat(), "__timezone__": str(obj.tzinfo) if obj.tzinfo else None}
        elif isinstance(obj, date):
            return {"__date__": obj.isoformat()}
        elif isinstance(obj, time):
            return {"__time__": obj.isoformat()}
        elif isinstance(obj, Decimal):
            return {"__decimal__": str(obj)}
        elif isinstance(obj, set):
            return {"__set__": list(obj)}
        elif isinstance(obj, frozenset):
            return {"__frozenset__": list(obj)}
        elif isinstance(obj, UUID):
            return {"__uuid__": str(obj)}
        elif isinstance(obj, Path):
            return {"__path__": str(obj)}
        return {"__fallback__": str(obj), "__type__": type(obj).__name__}
