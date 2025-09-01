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

import io
import logging
from typing import Any, List, Dict, Optional, Union, Type
from abc import ABC
from datetime import datetime, date, time
from decimal import Decimal
import uuid

# Manual serializer for types pyhessian can't handle
from .manual_hessian import Hessian2Output as output, Hessian2Input as input

try:
    from pyhessian import Hessian2Input, Hessian2Output
    _HAS_PYHESSIAN = True
except ImportError:
    _HAS_PYHESSIAN = False


class HessianTypeError(Exception):
    """Exception raised for type validation errors in Hessian RPC."""
    pass


class HessianRpcError(Exception):
    """Base exception for Hessian RPC errors."""
    pass


class TypeValidator:
    """Type validation utilities for Hessian RPC."""
    
    @staticmethod
    def validate_type(value: Any, expected_type: Union[str, Type]) -> bool:
        """Validate if a value matches the expected type."""
        if isinstance(expected_type, str):
            return TypeValidator._validate_string_type(value, expected_type)
        else:
            return isinstance(value, expected_type)
    
    @staticmethod
    def _validate_string_type(value: Any, type_string: str) -> bool:
        """Validate value against string type specification."""
        type_mapping = {
            'str': str, 'int': int, 'float': float, 'bool': bool,
            'list': list, 'dict': dict, 'bytes': bytes, 'tuple': tuple,
            'set': set, 'frozenset': frozenset, 'datetime': datetime,
            'date': date, 'time': time, 'decimal': Decimal, 'uuid': uuid.UUID,
            'none': type(None), 'any': object
        }
        
        if type_string.lower() in type_mapping:
            expected_type = type_mapping[type_string.lower()]
            return isinstance(value, expected_type)
        
        if '[' in type_string and ']' in type_string:
            base_type = type_string.split('[')[0].lower()
            if base_type == 'list':
                return isinstance(value, list)
            elif base_type == 'dict':
                return isinstance(value, dict)
        
        return True
    
    @staticmethod
    def validate_parameters(args: tuple, param_types: List[Union[str, Type]]) -> None:
        """Validate method parameters against expected types."""
        if len(args) != len(param_types):
            raise HessianTypeError(
                f"Parameter count mismatch: expected {len(param_types)}, got {len(args)}"
            )
        
        for i, (arg, expected_type) in enumerate(zip(args, param_types)):
            if not TypeValidator.validate_type(arg, expected_type):
                raise HessianTypeError(
                    f"Parameter {i} type mismatch: expected {expected_type}, got {type(arg).__name__}"
                )


class TypeProvider:
    """Provides type information and serialization hints for custom objects."""
    
    def __init__(self):
        self._type_registry: Dict[str, Type] = {}
    
    def register_type(self, type_name: str, type_class: Type) -> None:
        """Register a custom type for serialization."""
        self._type_registry[type_name] = type_class
    
    def get_type(self, type_name: str) -> Optional[Type]:
        """Get a registered type by name."""
        return self._type_registry.get(type_name)


class HybridHessianHandler:
    """Handles decision-making between pyhessian and manual serialization."""
    
    # Types that pyhessian typically can't handle well
    MANUAL_TYPES = {
        set, frozenset, tuple, Decimal, uuid.UUID, date, time, 
        complex, range, memoryview, bytearray
    }
    
    # Custom objects (non-builtin types)
    BUILTIN_TYPES = {
        str, int, float, bool, list, dict, bytes, type(None),
        datetime  # pyhessian can handle datetime
    }
    
    @classmethod
    def should_use_manual(cls, obj: Any) -> bool:
        """Determine if an object should use manual serialization."""
        obj_type = type(obj)
        
        # Check if it's a type we know needs manual handling
        if obj_type in cls.MANUAL_TYPES:
            return True
            
        # Check if it's a custom class (not a builtin)
        if obj_type not in cls.BUILTIN_TYPES and hasattr(obj, '__dict__'):
            return True
            
        # For containers, check their contents
        if isinstance(obj, (list, tuple)):
            return any(cls.should_use_manual(item) for item in obj)
        elif isinstance(obj, dict):
            return any(cls.should_use_manual(k) or cls.should_use_manual(v) 
                      for k, v in obj.items())
        elif isinstance(obj, (set, frozenset)):
            return True  # sets always need manual handling
            
        return False
    
    @classmethod
    def contains_manual_types(cls, args: tuple) -> bool:
        """Check if any argument requires manual serialization."""
        return any(cls.should_use_manual(arg) for arg in args)


class HessianTransportEncoder:
    """Encodes Python objects using hybrid pyhessian/manual approach."""

    def __init__(self, parameter_types: Optional[List[Union[str, Type]]] = None):
        self.parameter_types = parameter_types or []
        self._type_provider: Optional[TypeProvider] = None
        self._logger = logging.getLogger(__name__)

    def encode(self, arguments: tuple) -> bytes:
        """Encode arguments using the most appropriate serializer."""
        if self.parameter_types:
            TypeValidator.validate_parameters(arguments, self.parameter_types)
        
        try:
            # Decide which serializer to use
            use_manual = not _HAS_PYHESSIAN or HybridHessianHandler.contains_manual_types(arguments)
            
            if use_manual:
                return self._encode_manual(arguments)
            else:
                return self._encode_pyhessian(arguments)
                
        except Exception as e:
            self._logger.error(f"Encoding error: {e}")
            # Fallback to manual if pyhessian fails
            if not use_manual and _HAS_PYHESSIAN:
                self._logger.info("Falling back to manual serialization")
                try:
                    return self._encode_manual(arguments)
                except Exception as fallback_error:
                    raise HessianRpcError(f"Both serializers failed. Last error: {fallback_error}")
            raise HessianRpcError(f"Failed to encode parameters: {e}")

    def _encode_pyhessian(self, arguments: tuple) -> bytes:
        """Encode using pyhessian library."""
        output_stream = io.BytesIO()
        hessian_output = Hessian2Output(output_stream)
        
        for arg in arguments:
            hessian_output.write_object(arg)
            
        return output_stream.getvalue()

    def _encode_manual(self, arguments: tuple) -> bytes:
        """Encode using manual serializer."""
        output_stream = io.BytesIO()
        hessian_output = output(output_stream)
        
        for arg in arguments:
            hessian_output.write_object(arg)
            
        return output_stream.getvalue()
    
    def register_type_provider(self, provider: TypeProvider) -> None:
        """Register a type provider for custom type handling."""
        self._type_provider = provider


class HessianTransportDecoder:
    """Decodes Hessian binary format using hybrid approach."""

    def __init__(self, target_type: Optional[Union[str, Type]] = None, prefer_manual: bool = False):
        self.target_type = target_type
        self.prefer_manual = prefer_manual
        self._type_provider: Optional[TypeProvider] = None
        self._logger = logging.getLogger(__name__)

    def decode(self, data: bytes) -> Any:
        """Decode data using the most appropriate deserializer."""
        try:
            # Try the preferred method first
            if self.prefer_manual or not _HAS_PYHESSIAN:
                return self._decode_manual(data)
            else:
                return self._decode_pyhessian(data)
                
        except Exception as e:
            self._logger.error(f"Decoding error: {e}")
            # Fallback to the other method
            if not self.prefer_manual and _HAS_PYHESSIAN:
                self._logger.info("Falling back to manual deserialization")
                try:
                    return self._decode_manual(data)
                except Exception as fallback_error:
                    raise HessianRpcError(f"Both deserializers failed. Last error: {fallback_error}")
            elif self.prefer_manual and _HAS_PYHESSIAN:
                self._logger.info("Falling back to pyhessian deserialization")
                try:
                    return self._decode_pyhessian(data)
                except Exception as fallback_error:
                    raise HessianRpcError(f"Both deserializers failed. Last error: {fallback_error}")
            raise HessianRpcError(f"Failed to decode data: {e}")

    def _decode_pyhessian(self, data: bytes) -> Any:
        """Decode using pyhessian library."""
        input_stream = io.BytesIO(data)
        hessian_input = Hessian2Input(input_stream)
        result = hessian_input.read_object()
        
        if self.target_type:
            if not TypeValidator.validate_type(result, self.target_type):
                raise HessianTypeError(
                    f"Return type mismatch: expected {self.target_type}, got {type(result).__name__}"
                )
        
        return result

    def _decode_manual(self, data: bytes) -> Any:
        """Decode using manual deserializer."""
        input_stream = io.BytesIO(data)
        hessian_input = input(input_stream)
        result = hessian_input.read_object()
        
        if self.target_type:
            if not TypeValidator.validate_type(result, self.target_type):
                raise HessianTypeError(
                    f"Return type mismatch: expected {self.target_type}, got {type(result).__name__}"
                )
        
        return result
    
    def register_type_provider(self, provider: TypeProvider) -> None:
        """Register a type provider for custom type handling."""
        self._type_provider = provider


class HessianTransportCodec(ABC):
    """High-level encoder/decoder wrapper with hybrid serialization."""

    def __init__(self, parameter_types: Optional[List[Union[str, Type]]] = None,
                 return_type: Optional[Union[str, Type]] = None,
                 prefer_manual: bool = False):
        self.parameter_types = parameter_types or []
        self.return_type = return_type
        self.prefer_manual = prefer_manual
        self._encoder = HessianTransportEncoder(parameter_types)
        self._decoder = HessianTransportDecoder(return_type, prefer_manual)
        self._logger = logging.getLogger(__name__)

    def encode_parameters(self, *arguments) -> bytes:
        """Encode method parameters to Hessian binary format."""
        return self._encoder.encode(arguments)

    def decode_return_value(self, data: bytes) -> Any:
        """Decode return value from Hessian binary format."""
        return self._decoder.decode(data)
    
    def validate_call(self, *arguments) -> None:
        """Validate method call parameters without encoding."""
        if self.parameter_types:
            TypeValidator.validate_parameters(arguments, self.parameter_types)
    
    def register_type_provider(self, provider: TypeProvider) -> None:
        """Register a type provider for both encoder and decoder."""
        self._encoder.register_type_provider(provider)
        self._decoder.register_type_provider(provider)
    
    def get_encoder(self) -> HessianTransportEncoder:
        """Get the encoder instance."""
        return self._encoder
    
    def get_decoder(self) -> HessianTransportDecoder:
        """Get the decoder instance."""
        return self._decoder
    
    def set_manual_preference(self, prefer_manual: bool)-> None:
        """Set preference for manual serialization."""
        self.prefer_manual = prefer_manual
        self._decoder.prefer_manual = prefer_manual
