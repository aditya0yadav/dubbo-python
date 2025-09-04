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

import json
from typing import Any, Type, List, Union, Dict
from datetime import datetime, date, time
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from enum import Enum
from dataclasses import is_dataclass, asdict
from typing import Any, Dict, List


class StandardJsonPlugin:
    """Standard library JSON plugin"""

    def encode(self, obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    def can_handle(self, obj: Any) -> bool:
        return True


class OrJsonPlugin:
    """orjson plugin independent implementation"""

    def __init__(self):
        try:
            import orjson

            self.orjson = orjson
            self.available = True
        except ImportError:
            self.available = False

    def encode(self, obj: Any) -> bytes:
        if not self.available:
            raise ImportError("orjson not available")
        return self.orjson.dumps(obj, default=self._default_handler)

    def decode(self, data: bytes) -> Any:
        if not self.available:
            raise ImportError("orjson not available")
        return self.orjson.loads(data)

    def can_handle(self, obj: Any) -> bool:
        return self.available

    def _default_handler(self, obj):
        """Handle types not supported natively by orjson"""
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


class UJsonPlugin:
    """ujson plugin implementation"""

    def __init__(self):
        try:
            import ujson

            self.ujson = ujson
            self.available = True
        except ImportError:
            self.available = False

    def encode(self, obj: Any) -> bytes:
        if not self.available:
            raise ImportError("ujson not available")
        return self.ujson.dumps(obj, ensure_ascii=False, default=self._default_handler).encode("utf-8")

    def decode(self, data: bytes) -> Any:
        if not self.available:
            raise ImportError("ujson not available")
        return self.ujson.loads(data.decode("utf-8"))

    def can_handle(self, obj: Any) -> bool:
        return self.available

    def _default_handler(self, obj):
        """Handle types not supported natively by ujson"""
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


class DateTimeHandler:
    """DateTime handler - implements TypeHandlerPlugin protocol"""

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return isinstance(obj, (datetime, date, time))

    def serialize_to_dict(self, obj: Union[datetime, date, time]) -> Dict[str, str]:
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat(), "__timezone__": str(obj.tzinfo) if obj.tzinfo else None}
        elif isinstance(obj, date):
            return {"__date__": obj.isoformat()}
        else:
            return {"__time__": obj.isoformat()}


class DecimalHandler:
    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return obj_type is Decimal

    def serialize_to_dict(self, obj: Decimal) -> Dict[str, str]:
        return {"__decimal__": str(obj)}


class CollectionHandler:
    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return obj_type in (set, frozenset)

    def serialize_to_dict(self, obj: Union[set, frozenset]) -> Dict[str, List]:
        return {"__frozenset__" if isinstance(obj, frozenset) else "__set__": list(obj)}


class EnumHandler:
    """Handles serialization of Enum types"""

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return isinstance(obj, Enum)

    def serialize_to_dict(self, obj: Enum) -> Dict[str, Any]:
        return {"__enum__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}", "value": obj.value}


class DataclassHandler:
    """Handles serialization of dataclass types"""

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return is_dataclass(obj)

    def serialize_to_dict(self, obj: Any) -> Dict[str, Any]:
        return {"__dataclass__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}", "fields": asdict(obj)}


class SimpleTypeHandler:
    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return obj_type in (UUID, Path) or isinstance(obj, Path)

    def serialize_to_dict(self, obj: Union[UUID, Path]) -> Dict[str, str]:
        if isinstance(obj, UUID):
            return {"__uuid__": str(obj)}
        elif isinstance(obj, Path):
            return {"__path__": str(obj)}


class PydanticHandler:
    """Separate Pydantic plugin with enhanced features"""

    def __init__(self):
        try:
            from pydantic import BaseModel, create_model

            self.BaseModel = BaseModel
            self.create_model = create_model
            self.available = True
        except ImportError:
            self.available = False

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        return self.available and isinstance(obj, self.BaseModel)

    def serialize_to_dict(self, obj) -> Dict[str, Any]:
        if hasattr(obj, "model_dump"):
            return {
                "__pydantic_model__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}",
                "__model_data__": obj.model_dump(),
            }
        return {
            "__pydantic_model__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}",
            "__model_data__": obj.dict(),
        }

    def create_parameter_model(self, parameter_types: List[Type]):
        """Enhanced parameter handling for both positional and keyword args"""
        if not self.available:
            return None

        model_fields = {}
        for i, param_type in enumerate(parameter_types):
            model_fields[f"param_{i}"] = (param_type, ...)
        return self.create_model("ParametersModel", **model_fields)
