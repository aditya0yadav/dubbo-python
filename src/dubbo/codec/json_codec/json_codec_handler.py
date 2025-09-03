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

from typing import Any, Type, List, Union, Optional
from .json_transport_base import SimpleRegistry, SerializationException, DeserializationException
from .json_transport_plugins import (
    StandardJsonPlugin,
    OrJsonPlugin,
    UJsonPlugin,
    DateTimeHandler,
    DecimalHandler,
    CollectionHandler,
    SimpleTypeHandler,
    PydanticHandler,
    DataclassHandler,
    EnumHandler,
)


class JsonTransportEncoder:
    """JSON Transport Encoder with plugin architecture"""

    def __init__(
        self, parameter_types: List[Type] = None, maximum_depth: int = 100, strict_validation: bool = True, **kwargs
    ):
        self.parameter_types = parameter_types or []
        self.maximum_depth = maximum_depth
        self.strict_validation = strict_validation
        self.registry = SimpleRegistry()
        self.json_plugins = []

        # Setup plugins
        self._register_default_type_plugins()
        self._setup_json_serializer_plugins()

    def _register_default_type_plugins(self):
        """Register default type handler plugins"""
        default_plugins = [
            DateTimeHandler(),
            DecimalHandler(),
            CollectionHandler(),
            SimpleTypeHandler(),
            DataclassHandler(),
            EnumHandler(),
        ]

        # Add Pydantic plugin if available
        pydantic_plugin = PydanticHandler()
        if pydantic_plugin.available:
            default_plugins.append(pydantic_plugin)

        for plugin in default_plugins:
            self.registry.register_plugin(plugin)

    def _setup_json_serializer_plugins(self):
        """Setup JSON serializer plugins in priority order"""
        # Try orjson first (fastest), then ujson, finally standard json
        orjson_plugin = OrJsonPlugin()
        if orjson_plugin.available:
            self.json_plugins.append(orjson_plugin)

        ujson_plugin = UJsonPlugin()
        if ujson_plugin.available:
            self.json_plugins.append(ujson_plugin)

        # Always have standard json as fallback
        self.json_plugins.append(StandardJsonPlugin())

    def register_type_provider(self, provider):
        """Register custom type provider for backward compatibility"""
        self.registry.register_plugin(provider)

    def encode(self, arguments: tuple) -> bytes:
        """Encode arguments with flexible parameter handling"""
        try:
            if not arguments:
                return self._serialize_to_json_bytes([])

            # Handle single parameter case
            if len(self.parameter_types) == 1:
                parameter = arguments[0]
                serialized_param = self._serialize_object(parameter)
                return self._serialize_to_json_bytes(serialized_param)

            # Handle multiple parameters
            elif len(self.parameter_types) > 1:
                # Try Pydantic wrapper for strong typing
                pydantic_handler = self._get_pydantic_handler()
                if pydantic_handler and pydantic_handler.available:
                    wrapper_data = {f"param_{i}": arg for i, arg in enumerate(arguments)}
                    wrapper_model = pydantic_handler.create_parameter_model(self.parameter_types)
                    if wrapper_model:
                        try:
                            wrapper_instance = wrapper_model(**wrapper_data)
                            return self._serialize_to_json_bytes(pydantic_handler.serialize_to_dict(wrapper_instance))
                        except Exception:
                            pass  # Fall back to standard handling

                # Standard multi-parameter handling
                serialized_args = [self._serialize_object(arg) for arg in arguments]
                return self._serialize_to_json_bytes(serialized_args)

            else:
                # No type constraints - serialize as list
                serialized_args = [self._serialize_object(arg) for arg in arguments]
                return self._serialize_to_json_bytes(serialized_args)

        except Exception as e:
            raise SerializationException(f"Encoding failed: {e}") from e

    def _get_pydantic_handler(self) -> Optional[PydanticHandler]:
        """Get Pydantic handler from registered plugins"""
        for plugin in self.registry.plugins:
            if isinstance(plugin, PydanticHandler):
                return plugin
        return None

    def _serialize_object(self, obj: Any, depth: int = 0) -> Any:
        """Serialize single object using registry with depth protection"""
        if depth > self.maximum_depth:
            raise SerializationException(f"Maximum depth {self.maximum_depth} exceeded")

        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj

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

        # Use registry to find handler
        handler = self.registry.get_handler(obj)
        if handler:
            try:
                return handler(obj)
            except Exception as e:
                if self.strict_validation:
                    raise SerializationException(f"Handler failed for {type(obj).__name__}: {e}") from e
                return {"__serialization_error__": str(e), "__type__": type(obj).__name__}

        # Fallback for unknown types
        if self.strict_validation:
            raise SerializationException(f"No handler for type {type(obj).__name__}")
        return {"__fallback__": str(obj), "__type__": type(obj).__name__}

    def _serialize_to_json_bytes(self, obj: Any) -> bytes:
        """Use the first available JSON plugin to serialize"""
        last_error = None
        for plugin in self.json_plugins:
            try:
                return plugin.encode(obj)
            except Exception as e:
                last_error = e
                continue

        raise SerializationException(f"All JSON plugins failed. Last error: {last_error}")


class JsonTransportDecoder:
    """JSON Transport Decoder with plugin architecture"""

    def __init__(self, target_type: Union[Type, List[Type]] = None, **kwargs):
        self.target_type = target_type
        self.json_plugins = []
        self._setup_json_deserializer_plugins()

        # Handle multiple parameter types
        if isinstance(target_type, list):
            self.multiple_parameter_mode = len(target_type) > 1
            self.parameter_types = target_type
            if self.multiple_parameter_mode:
                pydantic_handler = PydanticHandler()
                if pydantic_handler.available:
                    self.parameter_wrapper_model = pydantic_handler.create_parameter_model(target_type)
        else:
            self.multiple_parameter_mode = False
            self.parameter_types = [target_type] if target_type else []

    def _setup_json_deserializer_plugins(self):
        """Setup JSON deserializer plugins in priority order"""
        orjson_plugin = OrJsonPlugin()
        if orjson_plugin.available:
            self.json_plugins.append(orjson_plugin)

        ujson_plugin = UJsonPlugin()
        if ujson_plugin.available:
            self.json_plugins.append(ujson_plugin)

        self.json_plugins.append(StandardJsonPlugin())

    def decode(self, data: bytes) -> Any:
        """Decode JSON bytes back to objects"""
        try:
            if not data:
                return None

            json_data = self._deserialize_from_json_bytes(data)
            reconstructed_data = self._reconstruct_objects(json_data)

            if not self.target_type:
                return reconstructed_data

            if isinstance(self.target_type, list):
                if self.multiple_parameter_mode and hasattr(self, "parameter_wrapper_model"):
                    try:
                        wrapper_instance = self.parameter_wrapper_model(**reconstructed_data)
                        return tuple(getattr(wrapper_instance, f"param_{i}") for i in range(len(self.parameter_types)))
                    except Exception:
                        pass
                return self._decode_to_target_type(reconstructed_data, self.parameter_types[0])
            else:
                return self._decode_to_target_type(reconstructed_data, self.target_type)

        except Exception as e:
            raise DeserializationException(f"Decoding failed: {e}") from e

    def _deserialize_from_json_bytes(self, data: bytes) -> Any:
        """Use the first available JSON plugin to deserialize"""
        last_error = None
        for plugin in self.json_plugins:
            try:
                return plugin.decode(data)
            except Exception as e:
                last_error = e
                continue

        raise DeserializationException(f"All JSON plugins failed. Last error: {last_error}")

    def _decode_to_target_type(self, json_data: Any, target_type: Type) -> Any:
        """Convert JSON data to target type with proper Pydantic handling"""
        # Check if target type is a Pydantic model
        try:
            from pydantic import BaseModel

            if isinstance(target_type, type) and issubclass(target_type, BaseModel):
                # If json_data is already a Pydantic model instance, return as-is
                if isinstance(json_data, target_type):
                    return json_data
                # If json_data is a dict, try to construct the Pydantic model
                elif isinstance(json_data, dict):
                    return target_type(**json_data)
        except ImportError:
            pass

        if target_type in (str, int, float, bool, list, dict):
            return target_type(json_data)

        return json_data

    def _reconstruct_objects(self, data: Any) -> Any:
        """Reconstruct special objects from their serialized form"""
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
            # Properly reconstruct Pydantic models
            return self._reconstruct_pydantic_model(data)
        elif "__dataclass__" in data:
            module_name, class_name = data["__dataclass__"].rsplit(".", 1)
            import importlib

            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            fields = self._reconstruct_objects(data["fields"])
            return cls(**fields)

        elif "__enum__" in data:
            module_name, class_name = data["__enum__"].rsplit(".", 1)
            import importlib

            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            return cls(data["value"])
        else:
            return {key: self._reconstruct_objects(value) for key, value in data.items()}

    def _reconstruct_pydantic_model(self, data: dict) -> Any:
        """Reconstruct a Pydantic model from serialized data"""
        try:
            model_path = data["__pydantic_model__"]
            model_data = data["__model_data__"]

            # Try to import and reconstruct the model
            module_name, class_name = model_path.rsplit(".", 1)

            # Import the module
            import importlib

            module = importlib.import_module(module_name)
            model_class = getattr(module, class_name)

            # Recursively reconstruct nested objects in model_data
            reconstructed_data = self._reconstruct_objects(model_data)

            # Create the model instance
            return model_class(**reconstructed_data)

        except Exception as e:
            # If reconstruction fails, return the model data as dict
            # This allows the target type conversion to handle it
            return self._reconstruct_objects(data.get("__model_data__", {}))


class JsonTransportCodec:
    """Combined JSON transport codec - maintains backward compatibility"""

    def __init__(
        self,
        parameter_types: List[Type] = None,
        return_type: Type = None,
        maximum_depth: int = 100,
        strict_validation: bool = True,
        **kwargs,
    ):
        self.parameter_types = parameter_types or []
        self.return_type = return_type
        self.maximum_depth = maximum_depth
        self.strict_validation = strict_validation

        self._encoder = JsonTransportEncoder(
            parameter_types=parameter_types, maximum_depth=maximum_depth, strict_validation=strict_validation, **kwargs
        )
        self._decoder = JsonTransportDecoder(target_type=return_type, **kwargs)

    def encode_parameters(self, *arguments) -> bytes:
        """Encode parameters - supports both positional and keyword args"""
        return self._encoder.encode(arguments)

    def decode_return_value(self, data: bytes) -> Any:
        """Decode return value"""
        return self._decoder.decode(data)

    def get_encoder(self) -> JsonTransportEncoder:
        return self._encoder

    def get_decoder(self) -> JsonTransportDecoder:
        return self._decoder

    def register_type_provider(self, provider) -> None:
        """Register custom type provider"""
        self._encoder.register_type_provider(provider)


class JsonTransportService:
    """Service for extension loader integration"""

    def __init__(self, parameter_types: List[Type] = None, return_type: Type = None, **kwargs):
        self.codec = JsonTransportCodec(parameter_types=parameter_types, return_type=return_type, **kwargs)

    def encode_parameters(self, *args, **kwargs) -> bytes:
        """Enhanced to handle both positional and keyword arguments"""
        if kwargs:
            # Convert keyword args to positional for now
            # Could be enhanced to preserve keyword information
            combined_args = args + tuple(kwargs.values())
            return self.codec.encode_parameters(*combined_args)
        return self.codec.encode_parameters(*args)

    def decode_return_value(self, data: bytes) -> Any:
        """Decode return value"""
        return self.codec.decode_return_value(data)

    def register_custom_plugin(self, plugin):
        """Allow extension loader to register custom plugins"""
        self.codec.register_type_provider(plugin)
