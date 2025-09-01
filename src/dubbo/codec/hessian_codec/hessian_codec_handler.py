import io
import struct
import logging
from typing import Any, List, Dict, Optional, Union, Type
from abc import ABC

try:
    from pyhessian import Hessian2Input, Hessian2Output
    
    _HAS_PYHESSIAN = True
except ImportError:
    _HAS_PYHESSIAN = False
    from .manual_hessian import Hessian2Input, Hessian2Output



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
            'list': list, 'dict': dict, 'bytes': bytes,
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



class HessianTransportEncoder:
    """Encodes Python objects into Hessian binary format with type validation."""

    def __init__(self, parameter_types: Optional[List[Union[str, Type]]] = None):
        self.parameter_types = parameter_types or []
        self._type_provider: Optional[TypeProvider] = None
        self._logger = logging.getLogger(__name__)

    def encode(self, arguments: tuple) -> bytes:
        if self.parameter_types:
            TypeValidator.validate_parameters(arguments, self.parameter_types)
        
        try:
            output_stream = io.BytesIO()
            hessian_output = Hessian2Output(output_stream)

            for arg in arguments:
                hessian_output.write_object(arg)

            return output_stream.getvalue()
        except Exception as e:
            self._logger.error(f"Encoding error: {e}")
            raise HessianRpcError(f"Failed to encode parameters: {e}")
    
    def register_type_provider(self, provider: TypeProvider) -> None:
        """Register a type provider for custom type handling."""
        self._type_provider = provider


class HessianTransportDecoder:
    """Decodes Hessian binary format into Python objects with type validation."""

    def __init__(self, target_type: Optional[Union[str, Type]] = None):
        self.target_type = target_type
        self._type_provider: Optional[TypeProvider] = None
        self._logger = logging.getLogger(__name__)

    def decode(self, data: bytes) -> Any:
        try:
            input_stream = io.BytesIO(data)
            hessian_input = Hessian2Input(input_stream)
            result = hessian_input.read_object()
            
            if self.target_type:
                if not TypeValidator.validate_type(result, self.target_type):
                    raise HessianTypeError(
                        f"Return type mismatch: expected {self.target_type}, got {type(result).__name__}"
                    )
            
            return result
        except Exception as e:
            self._logger.error(f"Decoding error: {e}")
            raise HessianRpcError(f"Failed to decode data: {e}")
    
    def register_type_provider(self, provider: TypeProvider) -> None:
        """Register a type provider for custom type handling."""
        self._type_provider = provider


class HessianTransportCodec(ABC):
    """High-level encoder/decoder wrapper for Hessian RPC with enhanced features."""

    def __init__(self, parameter_types: Optional[List[Union[str, Type]]] = None,
                 return_type: Optional[Union[str, Type]] = None):
        self.parameter_types = parameter_types or []
        self.return_type = return_type
        self._encoder = HessianTransportEncoder(parameter_types)
        self._decoder = HessianTransportDecoder(return_type)
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
