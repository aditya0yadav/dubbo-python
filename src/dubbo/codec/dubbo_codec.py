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

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class ParameterDescriptor:
    """Information about a method parameter"""

    name: str
    annotation: Any
    is_required: bool = True
    default_value: Any = None


@dataclass
class MethodDescriptor:
    """Method descriptor with function details"""

    function: Callable
    name: str
    parameters: list[ParameterDescriptor]
    return_parameter: ParameterDescriptor
    documentation: Optional[str] = None


class DubboSerializationService:
    """Dubbo serialization service with type handling"""

    @staticmethod
    def create_transport_codec(
        transport_type: str = "json",
        parameter_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        **codec_options,
    ):
        """Create transport codec"""

        try:
            from dubbo.classes import CodecHelper
            from dubbo.extension.extension_loader import ExtensionLoader

            codec_class = ExtensionLoader().get_extension(CodecHelper.get_class(), transport_type)
            return codec_class(parameter_types=parameter_types or [], return_type=return_type, **codec_options)
        except ImportError as e:
            logger.error("Failed to import required modules: %s", e)
            raise
        except Exception as e:
            logger.error("Failed to create transport codec: %s", e)
            raise

    @staticmethod
    def create_encoder_decoder_pair(
        transport_type: str,
        parameter_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        **codec_options,
    ) -> tuple[Any, Any]:
        """Create encoder and decoder instances"""

        try:
            codec_instance = DubboSerializationService.create_transport_codec(
                transport_type=transport_type,
                parameter_types=parameter_types,
                return_type=return_type,
                **codec_options,
            )

            encoder = codec_instance.get_encoder()
            decoder = codec_instance.get_decoder()

            if encoder is None or decoder is None:
                raise ValueError(f"Codec for transport type '{transport_type}' returned None encoder/decoder")

            return encoder, decoder

        except Exception as e:
            logger.error("Failed to create encoder/decoder pair: %s", e)
            raise

    @staticmethod
    def create_serialization_functions(
        transport_type: str,
        parameter_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        **codec_options,
    ) -> tuple[Callable, Callable]:
        """Create serializer and deserializer functions"""

        try:
            parameter_encoder, return_decoder = DubboSerializationService.create_encoder_decoder_pair(
                transport_type=transport_type,
                parameter_types=parameter_types,
                return_type=return_type,
                **codec_options,
            )

            def serialize_method_parameters(*args) -> bytes:
                try:
                    return parameter_encoder.encode(args)
                except Exception as e:
                    logger.error("Failed to serialize parameters: %s", e)
                    raise

            def deserialize_method_return(data: bytes):
                if not isinstance(data, bytes):
                    raise TypeError(f"Expected bytes, got {type(data)}")
                try:
                    return return_decoder.decode(data)
                except Exception as e:
                    logger.error("Failed to deserialize return value: %s", e)
                    raise

            return serialize_method_parameters, deserialize_method_return

        except Exception as e:
            logger.error("Failed to create serialization functions: %s", e)
            raise

    @staticmethod
    def create_method_descriptor(
        func: Callable,
        method_name: Optional[str] = None,
        parameter_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        interface: Optional[Callable[..., Any]] = None,
    ) -> MethodDescriptor:
        """Create a method descriptor from function and configuration"""

        if not callable(func):
            raise TypeError("func must be callable")

        # Use interface signature if provided, otherwise use func signature
        target_function = interface if interface is not None else func
        name = method_name or target_function.__name__

        try:
            sig = inspect.signature(target_function)
        except ValueError as e:
            logger.error("Cannot inspect signature of %s: %s", target_function, e)
            raise

        parameters = []
        resolved_parameter_types = parameter_types or []
        param_index = 0

        for param_name, param in sig.parameters.items():
            # Skip 'self' parameter for methods
            if param_name == "self":
                continue

            # Get parameter type from provided types, annotation, or default to Any
            if param_index < len(resolved_parameter_types):
                param_type = resolved_parameter_types[param_index]
            elif param.annotation != inspect.Parameter.empty:
                param_type = param.annotation
            else:
                param_type = Any

            is_required = param.default == inspect.Parameter.empty
            default_value = param.default if not is_required else None

            parameters.append(
                ParameterDescriptor(
                    name=param_name,
                    annotation=param_type,
                    is_required=is_required,
                    default_value=default_value,
                )
            )

            param_index += 1

        # Resolve return type
        if return_type is not None:
            resolved_return_type = return_type
        elif sig.return_annotation != inspect.Signature.empty:
            resolved_return_type = sig.return_annotation
        else:
            resolved_return_type = Any

        return_parameter = ParameterDescriptor(name="return_value", annotation=resolved_return_type)

        return MethodDescriptor(
            function=func,
            name=name,
            parameters=parameters,
            return_parameter=return_parameter,
            documentation=inspect.getdoc(target_function),
        )
