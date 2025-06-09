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

from typing import Callable, Optional
from dubbo.classes import MethodDescriptor
from dubbo.codec import DubboCodec
from dubbo.types import (
    DeserializingFunction,
    RpcTypes,
    SerializingFunction,
)

__all__ = ["RpcMethodHandler", "RpcServiceHandler"]


class RpcMethodHandler:
    """
    Rpc method handler
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
        Get the method encode and decode
        :return: tuple of the encode and decode method 
        """
        return DubboCodec.get_serializer_deserializer(**kwargs)

    @classmethod
    def unary(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs
    ) -> "RpcMethodHandler":
        """
        Create a unary method handler
        :param method: the method.
        :type method: Callable
        :param method_name: the method name. If not provided, the method name will be used.
        :type method_name: Optional[str]
        :param request_deserializer: the request deserializer.
        :type request_deserializer: Optional[DeserializingFunction]
        :param response_serializer: the response serializer.
        :type response_serializer: Optional[SerializingFunction]
        :return: the unary method handler.
        :rtype: RpcMethodHandler
        """
        # Use custom serializers if provided, otherwise get from codec
        if request_deserializer is None or response_serializer is None:
            default_deserializer, default_serializer = cls.get_codec(**kwargs)
            request_deserializer = request_deserializer or default_deserializer
            response_serializer = response_serializer or default_serializer
            
        return cls(
            MethodDescriptor(
                callable_method=method,
                method_name=method_name or method.__name__,
                arg_serialization=(None, request_deserializer),
                return_serialization=(response_serializer, None),
                rpc_type=RpcTypes.UNARY.value,
            )
        )

    @classmethod
    def client_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs
    ):
        """
        Create a client stream method handler
        :param method: the method.
        :type method: Callable
        :param method_name: the method name. If not provided, the method name will be used.
        :type method_name: Optional[str]
        :param request_deserializer: the request deserializer.
        :type request_deserializer: Optional[DeserializingFunction]
        :param response_serializer: the response serializer.
        :type response_serializer: Optional[SerializingFunction]
        :return: the client stream method handler.
        :rtype: RpcMethodHandler
        """
        # Use custom serializers if provided, otherwise get from codec
        if request_deserializer is None or response_serializer is None:
            default_deserializer, default_serializer = cls.get_codec(**kwargs)
            request_deserializer = request_deserializer or default_deserializer
            response_serializer = response_serializer or default_serializer
            
        return cls(
            MethodDescriptor(
                callable_method=method,
                method_name=method_name or method.__name__,
                arg_serialization=(None, request_deserializer),
                return_serialization=(response_serializer, None),
                rpc_type=RpcTypes.CLIENT_STREAM.value,
            )
        )

    @classmethod
    def server_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs
    ):
        """
        Create a server stream method handler
        :param method: the method.
        :type method: Callable
        :param method_name: the method name. If not provided, the method name will be used.
        :type method_name: Optional[str]
        :param request_deserializer: the request deserializer.
        :type request_deserializer: Optional[DeserializingFunction]
        :param response_serializer: the response serializer.
        :type response_serializer: Optional[SerializingFunction]
        :return: the server stream method handler.
        :rtype: RpcMethodHandler
        """
        # Use custom serializers if provided, otherwise get from codec
        if request_deserializer is None or response_serializer is None:
            default_deserializer, default_serializer = cls.get_codec(**kwargs)
            request_deserializer = request_deserializer or default_deserializer
            response_serializer = response_serializer or default_serializer
            
        return cls(
            MethodDescriptor(
                callable_method=method,
                method_name=method_name or method.__name__,
                arg_serialization=(None, request_deserializer),
                return_serialization=(response_serializer, None),
                rpc_type=RpcTypes.SERVER_STREAM.value,
            )
        )

    @classmethod
    def bi_stream(
        cls,
        method: Callable,
        method_name: Optional[str] = None,
        request_deserializer: Optional[DeserializingFunction] = None,
        response_serializer: Optional[SerializingFunction] = None,
        **kwargs,
    ):
        """
        Create a bidi stream method handler
        :param method: the method.
        :type method: Callable
        :param method_name: the method name. If not provided, the method name will be used.
        :type method_name: Optional[str]
        :param request_deserializer: the request deserializer.
        :type request_deserializer: Optional[DeserializingFunction]
        :param response_serializer: the response serializer.
        :type response_serializer: Optional[SerializingFunction]
        :return: the bidi stream method handler.
        :rtype: RpcMethodHandler
        """
        # Use custom serializers if provided, otherwise get from codec
        if request_deserializer is None or response_serializer is None:
            default_deserializer, default_serializer = cls.get_codec(**kwargs)
            request_deserializer = request_deserializer or default_deserializer
            response_serializer = response_serializer or default_serializer
            
        return cls(
            MethodDescriptor(
                callable_method=method,
                method_name=method_name or method.__name__,
                arg_serialization=(None, request_deserializer),
                return_serialization=(response_serializer, None),
                rpc_type=RpcTypes.BI_STREAM.value,
            )
        )
    

class RpcServiceHandler:
    """
    Rpc service handler
    """

    __slots__ = ["_service_name", "_method_handlers"]

    def __init__(self, service_name: str, method_handlers: list[RpcMethodHandler]):
        """
        Initialize the RpcServiceHandler
        :param service_name: the name of the service.
        :type service_name: str
        :param method_handlers: the method handlers.
        :type method_handlers: List[RpcMethodHandler]
        """
        self._service_name = service_name
        self._method_handlers: dict[str, RpcMethodHandler] = {}

        for method_handler in method_handlers:
            method_name = method_handler.method_descriptor.get_method_name()
            self._method_handlers[method_name] = method_handler

    @property
    def service_name(self) -> str:
        """
        Get the service name
        :return: the service name
        :rtype: str
        """
        return self._service_name

    @property
    def method_handlers(self) -> dict[str, RpcMethodHandler]:
        """
        Get the method handlers
        :return: the method handlers
        :rtype: Dict[str, RpcMethodHandler]
        """
        return self._method_handlers