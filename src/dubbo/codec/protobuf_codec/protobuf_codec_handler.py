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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS"
# BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Protocol

# Betterproto imports
try:
    import betterproto

    HAS_BETTERPROTO = True
except ImportError:
    HAS_BETTERPROTO = False


class SerializationException(Exception):
    """Exception raised when encoding or serialization fails."""

    def __init__(self, message: str, *, cause: Exception = None):
        super().__init__(message)
        self.cause = cause


class DeserializationException(Exception):
    """Exception raised when decoding or deserialization fails."""

    def __init__(self, message: str, *, cause: Exception = None):
        super().__init__(message)
        self.cause = cause


class ProtobufEncodingFunction(Protocol):
    def __call__(self, obj: Any) -> bytes: ...


class ProtobufDecodingFunction(Protocol):
    def __call__(self, data: bytes) -> Any: ...


@dataclass
class ProtobufMethodDescriptor:
    """Protobuf-specific method descriptor for single parameter"""

    parameter_type: type
    return_type: type
    protobuf_message_type: type | None = None


# Abstract base classes for pluggable architecture
class TypeHandler(ABC):
    """Abstract base class for type handlers"""

    @abstractmethod
    def is_message(self, obj_type: type) -> bool:
        """Check if the type is a message type"""
        pass

    @abstractmethod
    def is_message_instance(self, obj: Any) -> bool:
        """Check if the object is a message instance"""
        pass

    @abstractmethod
    def is_compatible(self, obj_type: type) -> bool:
        """Check if the type is compatible with this handler"""
        pass


class EncodingStrategy(ABC):
    """Abstract base class for encoding strategies"""

    @abstractmethod
    def can_encode(self, parameter: Any, parameter_type: type | None = None) -> bool:
        """Check if this strategy can encode the given parameter"""
        pass

    @abstractmethod
    def encode(self, parameter: Any, parameter_type: type | None = None) -> bytes:
        """Encode the parameter to bytes"""
        pass


class DecodingStrategy(ABC):
    """Abstract base class for decoding strategies"""

    @abstractmethod
    def can_decode(self, data: bytes, target_type: type) -> bool:
        """Check if this strategy can decode to the target type"""
        pass

    @abstractmethod
    def decode(self, data: bytes, target_type: type) -> Any:
        """Decode the bytes to the target type"""
        pass


# Concrete implementations
class ProtobufTypeHandler(TypeHandler):
    """Handles type conversion between Python types and Betterproto"""

    def is_message(self, obj_type: type) -> bool:
        if not HAS_BETTERPROTO:
            return False
        try:
            return hasattr(obj_type, "__dataclass_fields__") and issubclass(obj_type, betterproto.Message)
        except (TypeError, AttributeError):
            return False

    def is_message_instance(self, obj: Any) -> bool:
        if not HAS_BETTERPROTO:
            return False
        return isinstance(obj, betterproto.Message)

    def is_compatible(self, obj_type: type) -> bool:
        return obj_type in (str, int, float, bool, bytes) or self.is_message(obj_type)

    # Static methods for backward compatibility
    @staticmethod
    def is_betterproto_message(obj_type: type) -> bool:
        handler = ProtobufTypeHandler()
        return handler.is_message(obj_type)

    @staticmethod
    def is_betterproto_message_instance(obj: Any) -> bool:
        handler = ProtobufTypeHandler()
        return handler.is_message_instance(obj)

    @staticmethod
    def is_protobuf_compatible(obj_type: type) -> bool:
        handler = ProtobufTypeHandler()
        return handler.is_compatible(obj_type)


class MessageEncodingStrategy(EncodingStrategy):
    """Encoding strategy for protobuf messages"""

    def __init__(self, type_handler: TypeHandler):
        self.type_handler = type_handler

    def can_encode(self, parameter: Any, parameter_type: type | None = None) -> bool:
        return self.type_handler.is_message_instance(parameter) or (
            parameter_type and self.type_handler.is_message(parameter_type)
        )

    def encode(self, parameter: Any, parameter_type: type | None = None) -> bytes:
        if self.type_handler.is_message_instance(parameter):
            return bytes(parameter)

        if parameter_type and self.type_handler.is_message(parameter_type):
            if isinstance(parameter, parameter_type):
                return bytes(parameter)
            elif isinstance(parameter, dict):
                try:
                    message = parameter_type().from_dict(parameter)
                    return bytes(message)
                except Exception as e:
                    raise SerializationException(f"Cannot convert dict to {parameter_type}: {e}")
            else:
                raise SerializationException(f"Cannot convert {type(parameter)} to {parameter_type}")

        raise SerializationException(f"Cannot encode {type(parameter)} as protobuf message")


class PrimitiveEncodingStrategy(EncodingStrategy):
    """Encoding strategy for primitive types"""

    def can_encode(self, parameter: Any, parameter_type: type | None = None) -> bool:
        return isinstance(parameter, (str, int, float, bool, bytes))

    def encode(self, parameter: Any, parameter_type: type | None = None) -> bytes:
        try:
            json_str = json.dumps({"value": parameter, "type": type(parameter).__name__})
            return json_str.encode("utf-8")
        except Exception as e:
            raise SerializationException(f"Failed to encode primitive {parameter}: {e}")


class MessageDecodingStrategy(DecodingStrategy):
    """Decoding strategy for protobuf messages"""

    def __init__(self, type_handler: TypeHandler):
        self.type_handler = type_handler

    def can_decode(self, data: bytes, target_type: type) -> bool:
        return self.type_handler.is_message(target_type)

    def decode(self, data: bytes, target_type: type) -> Any:
        try:
            return target_type().parse(data)
        except Exception as e:
            raise DeserializationException(f"Failed to parse betterproto message: {e}")


class PrimitiveDecodingStrategy(DecodingStrategy):
    """Decoding strategy for primitive types"""

    def can_decode(self, data: bytes, target_type: type) -> bool:
        return target_type in (str, int, float, bool, bytes)

    def decode(self, data: bytes, target_type: type) -> Any:
        try:
            json_str = data.decode("utf-8")
            parsed = json.loads(json_str)
            value = parsed.get("value")

            if target_type is str:
                return str(value)
            elif target_type is int:
                return int(value)
            elif target_type is float:
                return float(value)
            elif target_type is bool:
                return bool(value)
            elif target_type is bytes:
                return bytes(value) if isinstance(value, (list, bytes)) else str(value).encode()
            else:
                return value

        except Exception as e:
            raise DeserializationException(f"Failed to decode primitive: {e}")


class StrategyRegistry:
    """Registry for managing encoding/decoding strategies"""

    def __init__(self):
        self.encoding_strategies: list[EncodingStrategy] = []
        self.decoding_strategies: list[DecodingStrategy] = []

    def register_encoding_strategy(self, strategy: EncodingStrategy) -> None:
        """Register an encoding strategy"""
        self.encoding_strategies.append(strategy)

    def register_decoding_strategy(self, strategy: DecodingStrategy) -> None:
        """Register a decoding strategy"""
        self.decoding_strategies.append(strategy)

    def find_encoding_strategy(self, parameter: Any, parameter_type: type | None = None) -> Optional[EncodingStrategy]:
        """Find the first strategy that can encode the parameter"""
        for strategy in self.encoding_strategies:
            if strategy.can_encode(parameter, parameter_type):
                return strategy
        return None

    def find_decoding_strategy(self, data: bytes, target_type: type) -> Optional[DecodingStrategy]:
        """Find the first strategy that can decode to the target type"""
        for strategy in self.decoding_strategies:
            if strategy.can_decode(data, target_type):
                return strategy
        return None


class ProtobufTransportEncoder:
    """Protobuf encoder for single parameters using pluggable strategies"""

    def __init__(
        self,
        parameter_type: type = None,
        type_handler: TypeHandler = None,
        strategy_registry: StrategyRegistry = None,
        **kwargs,
    ):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportEncoder")
        self.parameter_type = parameter_type
        self.descriptor = ProtobufMethodDescriptor(parameter_type=parameter_type, return_type=None)

        self.type_handler = type_handler or ProtobufTypeHandler()
        self.strategy_registry = strategy_registry or self._create_default_registry()

    def _create_default_registry(self) -> StrategyRegistry:
        """Create default strategy registry with standard strategies"""
        registry = StrategyRegistry()
        registry.register_encoding_strategy(MessageEncodingStrategy(self.type_handler))
        registry.register_encoding_strategy(PrimitiveEncodingStrategy())
        return registry

    def encode(self, parameter: Any, parameter_type: type) -> bytes:
        try:
            if parameter is None:
                return b""

            if isinstance(parameter, tuple):
                if len(parameter) == 0:
                    return b""
                elif len(parameter) == 1:
                    return self._encode_single_parameter(parameter[0])
                else:
                    raise SerializationException(
                        f"Multiple parameters not supported. Got tuple with {len(parameter)} elements, expected 1."
                    )

            return self._encode_single_parameter(parameter)

        except Exception as e:
            raise SerializationException(f"Protobuf encoding failed: {e}") from e

    def _encode_single_parameter(self, parameter: Any) -> bytes:
        strategy = self.strategy_registry.find_encoding_strategy(parameter, self.parameter_type)
        if strategy:
            return strategy.encode(parameter, self.parameter_type)

        raise SerializationException(f"No encoding strategy found for {type(parameter)}")

    # Backward compatibility method
    def _encode_primitive(self, value: Any) -> bytes:
        strategy = PrimitiveEncodingStrategy()
        return strategy.encode(value)


class ProtobufTransportDecoder:
    """Protobuf decoder for single parameters using pluggable strategies"""

    def __init__(
        self,
        target_type: type = None,
        type_handler: TypeHandler = None,
        strategy_registry: StrategyRegistry = None,
        **kwargs,
    ):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportDecoder")

        self.target_type = target_type

        # Use provided components or create defaults
        self.type_handler = type_handler or ProtobufTypeHandler()
        self.strategy_registry = strategy_registry or self._create_default_registry()

    def _create_default_registry(self) -> StrategyRegistry:
        """Create default strategy registry with standard strategies"""
        registry = StrategyRegistry()
        registry.register_decoding_strategy(MessageDecodingStrategy(self.type_handler))
        registry.register_decoding_strategy(PrimitiveDecodingStrategy())
        return registry

    def decode(self, data: bytes) -> Any:
        try:
            if not data:
                return None

            if not self.target_type:
                raise DeserializationException("No target_type specified for decoding")

            return self._decode_single_parameter(data, self.target_type)

        except Exception as e:
            raise DeserializationException(f"Protobuf decoding failed: {e}") from e

    def _decode_single_parameter(self, data: bytes, target_type: type) -> Any:
        strategy = self.strategy_registry.find_decoding_strategy(data, target_type)
        if strategy:
            return strategy.decode(data, target_type)

        raise DeserializationException(f"No decoding strategy found for {target_type}")

    # Backward compatibility method
    def _decode_primitive(self, data: bytes, target_type: type) -> Any:
        strategy = PrimitiveDecodingStrategy()
        return strategy.decode(data, target_type)


class ProtobufTransportCodec:
    """Main protobuf codec class for single parameters"""

    def __init__(
        self,
        parameter_type: type = None,
        return_type: type = None,
        type_handler: TypeHandler = None,
        encoder_registry: StrategyRegistry = None,
        decoder_registry: StrategyRegistry = None,
        **kwargs,
    ):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportCodec")

        # Allow sharing registries between encoder and decoder, or use separate ones
        shared_registry = encoder_registry or decoder_registry

        self._encoder = ProtobufTransportEncoder(
            parameter_type=parameter_type,
            type_handler=type_handler,
            strategy_registry=encoder_registry or shared_registry,
            **kwargs,
        )
        self._decoder = ProtobufTransportDecoder(
            target_type=return_type,
            type_handler=type_handler,
            strategy_registry=decoder_registry or shared_registry,
            **kwargs,
        )

    def encode_parameter(self, argument: Any) -> bytes:
        return self._encoder.encode(argument)

    def encode_parameters(self, arguments: tuple) -> bytes:
        if not arguments:
            return b""
        if len(arguments) == 1:
            return self._encoder.encode(arguments[0])
        else:
            raise SerializationException(
                f"Multiple parameters not supported. Got {len(arguments)} arguments, expected 1."
            )

    def decode_return_value(self, data: bytes) -> Any:
        return self._decoder.decode(data)

    def get_encoder(self) -> ProtobufTransportEncoder:
        return self._encoder

    def get_decoder(self) -> ProtobufTransportDecoder:
        return self._decoder
