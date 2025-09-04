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
from typing import Any, Callable, Optional, get_type_hints

from dubbo.classes import MethodDescriptor
from dubbo.codec import DubboSerializationService
from dubbo.types import (
    DeserializingFunction,
    RpcTypes,
    SerializingFunction,
)

__all__ = ["RpcMethodHandler", "RpcServiceHandler"]


class RpcMethodConfigurationError(Exception):
    """
    Raised when RPC method is configured incorrectly.
    """

    pass


class RpcMethodHandler:
    """
    Rpc method handler that wraps metadata and serialization logic for a callable.
    """

    __slots__ = ["_method_descriptor"]

    def __init__(self, method_descriptor: MethodDescriptor):
        """
        Initialize the RpcMethodHandler
        :param method_descriptor: the method descriptor.
        :type method_descriptor: MethodDescriptor
        """
        self._method_descriptor = method_descriptor

    @property
    def method_descriptor(self) -> MethodDescriptor:
        """
        Get the method descriptor
        :return: the method descriptor
        :rtype: MethodDescriptor
        """
        return self._method_descriptor

    @staticmethod
    def get_codec(**kwargs) -> tuple:
        """
        Get the serialization and deserialization functions based on codec
        :param kwargs: codec settings like transport_type, parameter_types, return_type
        :return: serializer and deserializer functions
        :rtype: Tuple[SerializingFunction, DeserializingFunction]
        """
        return DubboSerializationService.create_serialization_functions(**kwargs)

    @classmethod
    def _infer_types_from_method(cls, method: Callable) -> tuple:
        """
        Infer method name, parameter types, and return type from a callable
        :param method: the method to analyze
        :type method: Callable
        :return: tuple of method name, parameter types, return type
        :rtype: Tuple[str, list[type], type]
        """
        try:
            type_hints = get_type_hints(method)
            sig = inspect.signature(method)
            method_name = method.__name__
            params = list(sig.parameters.values())

            if params and params[0].name == "self":
                raise RpcMethodConfigurationError(
                    f"Method '{method_name}' appears to be an unbound method with 'self' parameter. "
                    "RPC methods should be bound methods (e.g., instance.method) or standalone functions. "
                    "If you're registering a class method, ensure you pass a bound method: "
                    "RpcMethodHandler.unary(instance.method) not RpcMethodHandler.unary(Class.method)"
                )

            params_types = [type_hints.get(p.name, Any) for p in params]
            return_type = type_hints.get("return", Any)
            return method_name, params_types, return_type
        except RpcMethodConfigurationError:
            raise
        except Exception:
            return method.__name__, [Any], Any

    @classmethod
    def _create_method_descriptor(
        cls,
        method: Callable,
        method_name: str,
        params_types: list[type],
        return_type: type,
        rpc_type: str,
        codec: Optional[str] = None,
        param_encoder: Optional[DeserializingFunction] = None,
        return_decoder: Optional[SerializingFunction] = None,
        **kwargs,
    ) -> MethodDescriptor:
        """
        Create a MethodDescriptor with serialization configuration
        :param method: the actual function/method
        :param method_name: RPC method name
        :param params_types: parameter type hints
        :param return_type: return type hint
        :param rpc_type: type of RPC (unary, stream, etc.)
        :param codec: serialization codec (json, pb, etc.)
        :param param_encoder: deserialization function
        :param return_decoder: serialization function
        :param kwargs: additional codec args
        :return: MethodDescriptor instance
        :rtype: MethodDescriptor
        """
        if param_encoder is None or return_decoder is None:
            codec_kwargs = {
                "transport_type": codec or "json",
                "parameter_types": params_types,
                "return_type": return_type,
                **kwargs,
            }
            serializer, deserializer = cls.get_codec(**codec_kwargs)
            request_deserializer = param_encoder or deserializer
            response_serializer = return_decoder or serializer

        return MethodDescriptor(
            callable_method=method,
            method_name=method_name or method.__name__,
            arg_serialization=(None, request_deserializer),
            return_serialization=(response_serializer, None),
            rpc_type=rpc_type,
        )

    @classmethod
    def unary(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        params_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        codec: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs,
    ) -> "RpcMethodHandler":
        """
        Register a unary RPC method handler
        """
        inferred_name, inferred_param_types, inferred_return_type = cls._infer_types_from_method(method)
        resolved_method_name = method_name or inferred_name
        resolved_param_types = params_types or inferred_param_types
        resolved_return_type = return_type or inferred_return_type
        codec = codec or "json"

        return cls(
            cls._create_method_descriptor(
                method=method,
                method_name=resolved_method_name,
                params_types=resolved_param_types,
                return_type=resolved_return_type,
                rpc_type=RpcTypes.UNARY.value,
                codec=codec,
                request_deserializer=request_deserializer,
                response_serializer=response_serializer,
                **kwargs,
            )
        )

    @classmethod
    def client_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        params_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        codec: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs,
    ) -> "RpcMethodHandler":
        """
        Register a client-streaming RPC method handler
        """
        inferred_name, inferred_param_types, inferred_return_type = cls._infer_types_from_method(method)
        resolved_method_name = method_name or inferred_name
        resolved_param_types = params_types or inferred_param_types
        resolved_return_type = return_type or inferred_return_type
        resolved_codec = codec or "json"

        return cls(
            cls._create_method_descriptor(
                method=method,
                method_name=resolved_method_name,
                params_types=resolved_param_types,
                return_type=resolved_return_type,
                rpc_type=RpcTypes.CLIENT_STREAM.value,
                codec=resolved_codec,
                request_deserializer=request_deserializer,
                response_serializer=response_serializer,
                **kwargs,
            )
        )

    @classmethod
    def server_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        params_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        codec: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs,
    ) -> "RpcMethodHandler":
        """
        Register a server-streaming RPC method handler
        """
        inferred_name, inferred_param_types, inferred_return_type = cls._infer_types_from_method(method)
        resolved_method_name = method_name or inferred_name
        resolved_param_types = params_types or inferred_param_types
        resolved_return_type = return_type or inferred_return_type
        resolved_codec = codec or "json"

        return cls(
            cls._create_method_descriptor(
                method=method,
                method_name=resolved_method_name,
                params_types=resolved_param_types,
                return_type=resolved_return_type,
                rpc_type=RpcTypes.SERVER_STREAM.value,
                codec=resolved_codec,
                request_deserializer=request_deserializer,
                response_serializer=response_serializer,
                **kwargs,
            )
        )

    @classmethod
    def bi_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        params_types: Optional[list[type]] = None,
        return_type: Optional[type] = None,
        codec: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs,
    ) -> "RpcMethodHandler":
        """
        Register a bidirectional streaming RPC method handler
        """
        inferred_name, inferred_param_types, inferred_return_type = cls._infer_types_from_method(method)
        resolved_method_name = method_name or inferred_name
        resolved_param_types = params_types or inferred_param_types
        resolved_return_type = return_type or inferred_return_type
        resolved_codec = codec or "json"

        return cls(
            cls._create_method_descriptor(
                method=method,
                method_name=resolved_method_name,
                params_types=resolved_param_types,
                return_type=resolved_return_type,
                rpc_type=RpcTypes.BI_STREAM.value,
                codec=resolved_codec,
                request_deserializer=request_deserializer,
                response_serializer=response_serializer,
                **kwargs,
            )
        )


class RpcServiceHandler:
    """
    Rpc service handler that maps method names to their corresponding RpcMethodHandler.
    """

    __slots__ = ["_service_name", "_method_handlers"]

    def __init__(self, service_name: str, method_handlers: list[RpcMethodHandler]):
        """
        Initialize the RpcServiceHandler
        """
        self._service_name = service_name
        self._method_handlers: dict[str, RpcMethodHandler] = {}

        for method_handler in method_handlers:
            method_name = method_handler.method_descriptor.get_method_name()
            self._method_handlers[method_name] = method_handler

    @property
    def service_name(self) -> str:
        """Get the service name"""
        return self._service_name

    @property
    def method_handlers(self) -> dict[str, RpcMethodHandler]:
        """Get the registered RPC method handlers"""
        return self._method_handlers
