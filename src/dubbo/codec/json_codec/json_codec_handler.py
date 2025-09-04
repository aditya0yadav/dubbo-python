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

from typing import Any, Type, List, Optional
from dubbo.codec.json_codec import (
    JsonCodec,
    TypeHandler,
    StandardJsonCodec,
    OrJsonCodec,
    UJsonCodec,
    DateTimeHandler,
    PydanticHandler,
    CollectionHandler,
    DecimalHandler,
    SimpleTypesHandler,
    EnumHandler,
    DataclassHandler,
)

__all__ = ["JsonTransportCodec", "SerializationException", "DeserializationException"]


class SerializationException(Exception):
    """Exception raised during serialization"""

    pass


class DeserializationException(Exception):
    """Exception raised during deserialization"""

    pass


class JsonTransportCodec:
    """
    JSON Transport Codec
    """

    def __init__(
        self,
        parameter_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        maximum_depth: int = 100,
        strict_validation: bool = True,
        **kwargs,
    ):
        self.parameter_types = parameter_types or []
        self.return_type = return_type
        self.maximum_depth = maximum_depth
        self.strict_validation = strict_validation

        # Initialize codecs and handlers using the extension pattern
        self._json_codecs = self._setup_json_codecs()
        self._type_handlers = self._setup_type_handlers()

    def _setup_json_codecs(self) -> List[JsonCodec]:
        """
        Setup JSON codecs in priority order.
        Following the compression pattern: try fastest first, fallback to standard.
        """
        codecs = []

        # Try orjson first (fastest)
        orjson_codec = OrJsonCodec()
        if orjson_codec.can_handle(None):  # Check availability
            codecs.append(orjson_codec)

        # Try ujson second
        ujson_codec = UJsonCodec()
        if ujson_codec.can_handle(None):  # Check availability
            codecs.append(ujson_codec)

        # Always include standard json as fallback
        codecs.append(StandardJsonCodec())

        return codecs

    def _setup_type_handlers(self) -> List[TypeHandler]:
        """
        Setup type handlers for different object types.
        Similar to compression - each handler is independent and focused.
        """
        handlers = []

        # Add all available handlers
        handlers.append(DateTimeHandler())

        pydantic_handler = PydanticHandler()
        if pydantic_handler.available:
            handlers.append(pydantic_handler)

        handlers.extend(
            [
                DecimalHandler(),
                CollectionHandler(),
                SimpleTypesHandler(),
                EnumHandler(),
                DataclassHandler(),
            ]
        )

        return handlers

    def encode_parameters(self, *arguments) -> bytes:
        """
        Encode parameters to JSON bytes.

        :param arguments: The arguments to encode.
        :return: Encoded JSON bytes.
        :rtype: bytes
        """
        try:
            if not arguments:
                return self._encode_with_codecs([])

            # Handle single parameter case
            if len(self.parameter_types) == 1:
                serialized = self._serialize_object(arguments[0])
                return self._encode_with_codecs(serialized)

            # Handle multiple parameters
            elif len(self.parameter_types) > 1:
                serialized_args = [self._serialize_object(arg) for arg in arguments]
                return self._encode_with_codecs(serialized_args)

            # No type constraints
            else:
                if len(arguments) == 1:
                    serialized = self._serialize_object(arguments[0])
                    return self._encode_with_codecs(serialized)
                else:
                    serialized_args = [self._serialize_object(arg) for arg in arguments]
                    return self._encode_with_codecs(serialized_args)

        except Exception as e:
            raise SerializationException(f"Parameter encoding failed: {e}") from e

    def decode_return_value(self, data: bytes) -> Any:
        """
        Decode return value from JSON bytes.

        :param data: The JSON bytes to decode.
        :type data: bytes
        :return: Decoded return value.
        :rtype: Any
        """
        try:
            if not data:
                return None

            json_data = self._decode_with_codecs(data)
            return self._reconstruct_objects(json_data)

        except Exception as e:
            raise DeserializationException(f"Return value decoding failed: {e}") from e

    def _serialize_object(self, obj: Any, depth: int = 0) -> Any:
        """
        Serialize an object using the appropriate type handler.

        :param obj: The object to serialize.
        :param depth: Current serialization depth.
        :return: Serialized representation.
        """
        if depth > self.maximum_depth:
            raise SerializationException(f"Maximum depth {self.maximum_depth} exceeded")

        # Handle simple types
        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj

        # Handle collections
        if isinstance(obj, (list, tuple)):
            return [self._serialize_object(item, depth + 1) for item in obj]

        elif isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                if not isinstance(key, str):
                    if self.strict_validation:
                        raise SerializationException(f"Dictionary key must be string, got {type(key).__name__}")
                    key = str(key)
                result[key] = self._serialize_object(value, depth + 1)
            return result

        # Use type handlers for complex objects
        obj_type = type(obj)
        for handler in self._type_handlers:
            if handler.can_serialize_type(obj, obj_type):
                try:
                    serialized = handler.serialize_to_dict(obj)
                    return self._serialize_object(serialized, depth + 1)
                except Exception as e:
                    if self.strict_validation:
                        raise SerializationException(f"Handler failed for {type(obj).__name__}: {e}") from e
                    return {"__serialization_error__": str(e), "__type__": type(obj).__name__}

        # Fallback for unknown types
        if self.strict_validation:
            raise SerializationException(f"No handler for type {type(obj).__name__}")
        return {"__fallback__": str(obj), "__type__": type(obj).__name__}

    def _encode_with_codecs(self, obj: Any) -> bytes:
        """
        Encode object using the first available JSON codec.

        :param obj: The object to encode.
        :return: JSON bytes.
        :rtype: bytes
        """
        last_error = None

        for codec in self._json_codecs:
            try:
                return codec.encode(obj)
            except Exception as e:
                last_error = e
                continue

        raise SerializationException(f"All JSON codecs failed. Last error: {last_error}")

    def _decode_with_codecs(self, data: bytes) -> Any:
        """
        Decode JSON bytes using the first available codec.

        :param data: The JSON bytes to decode.
        :return: Decoded object.
        :rtype: Any
        """
        last_error = None

        for codec in self._json_codecs:
            try:
                return codec.decode(data)
            except Exception as e:
                last_error = e
                continue

        raise DeserializationException(f"All JSON codecs failed. Last error: {last_error}")

    def _reconstruct_objects(self, data: Any) -> Any:
        """
        Reconstruct objects from their serialized form.

        :param data: The data to reconstruct.
        :return: Reconstructed object.
        :rtype: Any
        """
        if not isinstance(data, dict):
            if isinstance(data, list):
                return [self._reconstruct_objects(item) for item in data]
            return data

        # Handle special serialized objects
        if "__datetime__" in data:
            from datetime import datetime

            return datetime.fromisoformat(data["__datetime__"])
        elif "__date__" in data:
            from datetime import date

            return date.fromisoformat(data["__date__"])
        elif "__time__" in data:
            from datetime import time

            return time.fromisoformat(data["__time__"])
        elif "__decimal__" in data:
            from decimal import Decimal

            return Decimal(data["__decimal__"])
        elif "__set__" in data:
            return set(self._reconstruct_objects(item) for item in data["__set__"])
        elif "__frozenset__" in data:
            return frozenset(self._reconstruct_objects(item) for item in data["__frozenset__"])
        elif "__uuid__" in data:
            from uuid import UUID

            return UUID(data["__uuid__"])
        elif "__path__" in data:
            from pathlib import Path

            return Path(data["__path__"])
        elif "__pydantic_model__" in data and "__model_data__" in data:
            return self._reconstruct_pydantic_model(data)
        elif "__dataclass__" in data:
            return self._reconstruct_dataclass(data)
        elif "__enum__" in data:
            return self._reconstruct_enum(data)
        else:
            return {key: self._reconstruct_objects(value) for key, value in data.items()}

    def _reconstruct_pydantic_model(self, data: dict) -> Any:
        """Reconstruct a Pydantic model from serialized data"""
        try:
            model_path = data["__pydantic_model__"]
            model_data = data["__model_data__"]

            module_name, class_name = model_path.rsplit(".", 1)

            import importlib

            module = importlib.import_module(module_name)
            model_class = getattr(module, class_name)

            reconstructed_data = self._reconstruct_objects(model_data)
            return model_class(**reconstructed_data)
        except Exception:
            return self._reconstruct_objects(data.get("__model_data__", {}))

    def _reconstruct_dataclass(self, data: dict) -> Any:
        """Reconstruct a dataclass from serialized data"""
        module_name, class_name = data["__dataclass__"].rsplit(".", 1)

        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)

        fields = self._reconstruct_objects(data["fields"])
        return cls(**fields)

    def _reconstruct_enum(self, data: dict) -> Any:
        """Reconstruct an enum from serialized data"""
        module_name, class_name = data["__enum__"].rsplit(".", 1)

        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)

        return cls(data["value"])
