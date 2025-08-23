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

from typing import Any, Type, Protocol, Optional
from abc import ABC, abstractmethod
import json
from dataclasses import dataclass

# Betterproto imports
try:
    import betterproto
    HAS_BETTERPROTO = True
except ImportError:
    HAS_BETTERPROTO = False

try:
    from pydantic import BaseModel
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

# Reuse your existing JSON type system
from dubbo.codec.json_codec.json_type import (
    TypeProviderFactory, SerializationState,
    SerializationException, DeserializationException
)


class ProtobufEncodingFunction(Protocol):
    def __call__(self, obj: Any) -> bytes: ...


class ProtobufDecodingFunction(Protocol):
    def __call__(self, data: bytes) -> Any: ...


@dataclass
class ProtobufMethodDescriptor:
    """Protobuf-specific method descriptor for single parameter"""
    parameter_type: Type
    return_type: Type
    protobuf_message_type: Optional[Type] = None
    use_json_fallback: bool = False


class ProtobufTypeHandler:
    """Handles type conversion between Python types and Betterproto"""

    @staticmethod
    def is_betterproto_message(obj_type: Type) -> bool:
        """Check if type is a betterproto message class"""
        if not HAS_BETTERPROTO:
            return False
        try:
            return (hasattr(obj_type, '__dataclass_fields__') and 
                    issubclass(obj_type, betterproto.Message))
        except (TypeError, AttributeError):
            return False

    @staticmethod
    def is_betterproto_message_instance(obj: Any) -> bool:
        """Check if object is a betterproto message instance"""
        if not HAS_BETTERPROTO:
            return False
        try:
            return isinstance(obj, betterproto.Message)
        except:
            return False

    @staticmethod
    def is_protobuf_compatible(obj_type: Type) -> bool:
        """Check if type can be handled by protobuf"""
        return (obj_type in (str, int, float, bool, bytes) or 
                ProtobufTypeHandler.is_betterproto_message(obj_type))

    @staticmethod
    def needs_json_fallback(parameter_type: Type) -> bool:
        """Check if we need JSON fallback for this type"""
        return not ProtobufTypeHandler.is_protobuf_compatible(parameter_type)


class ProtobufTransportEncoder:
    """Protobuf encoder for single parameters using betterproto"""

    def __init__(self, parameter_type: Type = None, **kwargs):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportEncoder")

        self.parameter_type = parameter_type
        
        self.descriptor = ProtobufMethodDescriptor(
            parameter_type=parameter_type,
            return_type=None,
            use_json_fallback=ProtobufTypeHandler.needs_json_fallback(parameter_type) if parameter_type else False
        )

        if self.descriptor.use_json_fallback:
            from dubbo.codec.json_codec.json_codec_handler import JsonTransportEncoder
            self.json_fallback_encoder = JsonTransportEncoder([parameter_type], **kwargs)

    def encode(self, parameter: Any) -> bytes:
        """Encode single parameter to bytes"""
        try:
            if parameter is None:
                return b''

            # Handle case where parameter is a tuple (common in RPC calls)
            if isinstance(parameter, tuple):
                if len(parameter) == 0:
                    return b''
                elif len(parameter) == 1:
                    return self._encode_single_parameter(parameter[0])
                else:
                    raise SerializationException(f"Multiple parameters not supported. Got tuple with {len(parameter)} elements, expected 1.")

            return self._encode_single_parameter(parameter)

        except Exception as e:
            raise SerializationException(f"Protobuf encoding failed: {e}") from e

    def _encode_single_parameter(self, parameter: Any) -> bytes:
        """Encode a single parameter using betterproto"""
        # If it's already a betterproto message instance, serialize it
        if ProtobufTypeHandler.is_betterproto_message_instance(parameter):
            return bytes(parameter)

        # If we have type info and it's a betterproto message type
        if self.parameter_type and ProtobufTypeHandler.is_betterproto_message(self.parameter_type):
            if isinstance(parameter, self.parameter_type):
                return bytes(parameter)
            elif isinstance(parameter, dict):
                # Convert dict to betterproto message
                try:
                    message = self.parameter_type().from_dict(parameter)
                    return bytes(message)
                except Exception as e:
                    raise SerializationException(f"Cannot convert dict to {self.parameter_type}: {e}")
            else:
                raise SerializationException(f"Cannot convert {type(parameter)} to {self.parameter_type}")

        # Handle primitive types by wrapping in a simple message
        if isinstance(parameter, (str, int, float, bool, bytes)):
            return self._encode_primitive(parameter)

        # Use JSON fallback if configured
        if self.descriptor.use_json_fallback:
            json_data = self.json_fallback_encoder.encode((parameter,))
            return json_data

        raise SerializationException(f"Cannot encode {type(parameter)} as protobuf")

    def _encode_primitive(self, value: Any) -> bytes:
        """Encode primitive values by wrapping them in a simple structure"""
        # For primitives, we'll use JSON encoding wrapped in bytes
        # This is a simplified approach - in a real implementation you might
        # want to define a wrapper protobuf message for primitives
        try:
            json_str = json.dumps({"value": value, "type": type(value).__name__})
            return json_str.encode('utf-8')
        except Exception as e:
            raise SerializationException(f"Failed to encode primitive {value}: {e}")


class ProtobufTransportDecoder:
    """Protobuf decoder for single parameters using betterproto"""

    def __init__(self, target_type: Type = None, **kwargs):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportDecoder")

        self.target_type = target_type
        self.use_json_fallback = ProtobufTypeHandler.needs_json_fallback(target_type) if target_type else False

        if self.use_json_fallback:
            from dubbo.codec.json_codec.json_codec_handler import JsonTransportDecoder
            self.json_fallback_decoder = JsonTransportDecoder(target_type, **kwargs)

    def decode(self, data: bytes) -> Any:
        """Decode bytes to single parameter"""
        try:
            if not data:
                return None

            if not self.target_type:
                return self._decode_without_type_info(data)

            return self._decode_single_parameter(data, self.target_type)

        except Exception as e:
            raise DeserializationException(f"Protobuf decoding failed: {e}") from e

    def _decode_single_parameter(self, data: bytes, target_type: Type) -> Any:
        """Decode single parameter using betterproto"""
        if ProtobufTypeHandler.is_betterproto_message(target_type):
            try:
                # Use betterproto's parsing
                message_instance = target_type().parse(data)
                return message_instance
            except Exception as e:
                if self.use_json_fallback:
                    return self.json_fallback_decoder.decode(data)
                raise DeserializationException(f"Failed to parse betterproto message: {e}")

        # Handle primitives
        elif target_type in (str, int, float, bool, bytes):
            return self._decode_primitive(data, target_type)

        # Use JSON fallback
        elif self.use_json_fallback:
            return self.json_fallback_decoder.decode(data)

        else:
            raise DeserializationException(f"Cannot decode to {target_type} from protobuf")

    def _decode_primitive(self, data: bytes, target_type: Type) -> Any:
        """Decode primitive values from their wrapped format"""
        try:
            json_str = data.decode('utf-8')
            parsed = json.loads(json_str)
            value = parsed.get("value")
            
            # Convert to target type if needed
            if target_type == str:
                return str(value)
            elif target_type == int:
                return int(value)
            elif target_type == float:
                return float(value)
            elif target_type == bool:
                return bool(value)
            elif target_type == bytes:
                return bytes(value) if isinstance(value, (list, bytes)) else str(value).encode()
            else:
                return value
                
        except Exception as e:
            raise DeserializationException(f"Failed to decode primitive: {e}")

    def _decode_without_type_info(self, data: bytes) -> Any:
        """Decode without type information - try JSON first"""
        try:
            return json.loads(data.decode('utf-8'))
        except:
            return data


class ProtobufTransportCodec:
    """Main protobuf codec class for single parameters using betterproto"""

    def __init__(self, parameter_type: Type = None, return_type: Type = None, **kwargs):
        if not HAS_BETTERPROTO:
            raise ImportError("betterproto library is required for ProtobufTransportCodec")

        self.parameter_type = parameter_type
        self.return_type = return_type

        self._encoder = ProtobufTransportEncoder(
            parameter_type=parameter_type,
            **kwargs
        )
        self._decoder = ProtobufTransportDecoder(
            target_type=return_type,
            **kwargs
        )

    def encode_parameter(self, argument: Any) -> bytes:
        """Encode single parameter"""
        return self._encoder.encode(argument)
    
    def encode_parameters(self, arguments: tuple) -> bytes:
        """Legacy method to handle tuple of arguments (for backward compatibility)"""
        if not arguments:
            return b''
        if len(arguments) == 1:
            return self._encoder.encode(arguments[0])
        else:
            raise SerializationException(f"Multiple parameters not supported. Got {len(arguments)} arguments, expected 1.")

    def decode_return_value(self, data: bytes) -> Any:
        """Decode return value"""
        return self._decoder.decode(data)

    def get_encoder(self) -> ProtobufTransportEncoder:
        return self._encoder

    def get_decoder(self) -> ProtobufTransportDecoder:
        return self._decoder


def create_protobuf_codec(**kwargs) -> ProtobufTransportCodec:
    """Factory function to create protobuf codec"""
    return ProtobufTransportCodec(**kwargs)