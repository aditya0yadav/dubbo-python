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

import threading
from typing import Optional, Callable, List, Type, Union, Any

from dubbo.bootstrap import Dubbo
from dubbo.classes import MethodDescriptor
from dubbo.configs import ReferenceConfig
from dubbo.constants import common_constants
from dubbo.extension import extensionLoader
from dubbo.protocol import Invoker, Protocol
from dubbo.proxy import RpcCallable, RpcCallableFactory
from dubbo.proxy.callables import DefaultRpcCallableFactory
from dubbo.registry.protocol import RegistryProtocol
from dubbo.types import (
    DeserializingFunction,
    RpcTypes,
    SerializingFunction,
)
from dubbo.url import URL
from dubbo.codec import DubboTransportService

__all__ = ["Client"]


class Client:
    def __init__(self, reference: ReferenceConfig, dubbo: Optional[Dubbo] = None):
        self._initialized = False
        self._global_lock = threading.RLock()

        self._dubbo = dubbo or Dubbo()
        self._reference = reference

        self._url: Optional[URL] = None
        self._protocol: Optional[Protocol] = None
        self._invoker: Optional[Invoker] = None

        self._callable_factory: RpcCallableFactory = DefaultRpcCallableFactory()

        # initialize the invoker
        self._initialize()

    def _initialize(self):
        """
        Initialize the invoker.
        """
        with self._global_lock:
            if self._initialized:
                return

            # get the protocol
            protocol = extensionLoader.get_extension(Protocol, self._reference.protocol)()

            registry_config = self._dubbo.registry_config

            self._protocol = RegistryProtocol(registry_config, protocol) if self._dubbo.registry_config else protocol

            # build url
            reference_url = self._reference.to_url()
            if registry_config:
                self._url = registry_config.to_url().copy()
                self._url.path = reference_url.path
                for k, v in reference_url.parameters.items():
                    self._url.parameters[k] = v
            else:
                self._url = reference_url

            # create invoker
            self._invoker = self._protocol.refer(self._url)

            self._initialized = True

    def _create_rpc_callable(
        self,
        rpc_type: str,
        interface: Optional[Callable] = None,
        method_name: Optional[str] = None,
        params_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        codec: Optional[str] = None,
        request_serializer: Optional[SerializingFunction] = None,
        response_deserializer: Optional[DeserializingFunction] = None,
        default_method_name: str = "rpc_call",
    ) -> RpcCallable:
        """
        Create RPC callable with the specified type.
        """
        # Validate
        if interface is None and method_name is None:
            raise ValueError("Either 'interface' or 'method_name' must be provided")

        # Determine the actual method name to call
        actual_method_name = method_name or (interface.__name__ if interface else default_method_name)
        
        # Build method descriptor (automatic or manual)
        if interface:
            method_desc = DubboTransportService.create_method_descriptor(
                func=interface,
                method_name=actual_method_name,
                parameter_types=params_types,
                return_type=return_type,
                interface=interface,
            )
        else:
            # Manual mode fallback: use dummy function for descriptor creation
            def dummy(): pass

            method_desc = DubboTransportService.create_method_descriptor(
                func=dummy,
                method_name=actual_method_name,
                parameter_types=params_types or [],
                return_type=return_type or Any,
            )

        # Determine serializers if not provided
        if request_serializer and response_deserializer:
            final_request_serializer = request_serializer
            final_response_deserializer = response_deserializer
        else:
            # Use DubboTransportService to generate serialization functions
            final_request_serializer, final_response_deserializer = DubboTransportService.create_serialization_functions(codec, parameter_types=params_types, return_type=return_type)
            print("final",codec, final_request_serializer, final_response_deserializer)

        # Create the proper MethodDescriptor for the RPC call
        rpc_method_descriptor = MethodDescriptor(
            method_name=actual_method_name,
            arg_serialization=(final_request_serializer, None),  # (serializer, deserializer) for arguments
            return_serialization=(None, final_response_deserializer),  # (serializer, deserializer) for return value
            rpc_type=rpc_type,
        )

        # Create and return the RpcCallable
        return self._callable(rpc_method_descriptor)

    def unary(
        self,
        interface: Optional[Callable] = None,
        method_name: Optional[str] = None,
        params_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        codec: Optional[str] = None,
        request_serializer: Optional[SerializingFunction] = None,
        response_deserializer: Optional[DeserializingFunction] = None,
    ) -> RpcCallable:
        """
        Create unary RPC call.

        Supports both automatic mode (via interface) and manual mode (via method_name + params_types + return_type + codec).
        """
        return self._create_rpc_callable(
            rpc_type=RpcTypes.UNARY.value,
            interface=interface,
            method_name=method_name,
            params_types=params_types,
            return_type=return_type,
            codec=codec,
            request_serializer=request_serializer,
            response_deserializer=response_deserializer,
            default_method_name="unary",
        )

    def client_stream(
        self,
        interface: Optional[Callable] = None,
        method_name: Optional[str] = None,
        params_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        codec: Optional[str] = None,
        request_serializer: Optional[SerializingFunction] = None,
        response_deserializer: Optional[DeserializingFunction] = None,
    ) -> RpcCallable:
        """
        Create client streaming RPC call.

        Supports both automatic mode (via interface) and manual mode (via method_name + params_types + return_type + codec).
        """
        return self._create_rpc_callable(
            rpc_type=RpcTypes.CLIENT_STREAM.value,
            interface=interface,
            method_name=method_name,
            params_types=params_types,
            return_type=return_type,
            codec=codec,
            request_serializer=request_serializer,
            response_deserializer=response_deserializer,
            default_method_name="client_stream",
        )

    def server_stream(
        self,
        interface: Optional[Callable] = None,
        method_name: Optional[str] = None,
        params_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        codec: Optional[str] = None,
        request_serializer: Optional[SerializingFunction] = None,
        response_deserializer: Optional[DeserializingFunction] = None,
    ) -> RpcCallable:
        """
        Create server streaming RPC call.

        Supports both automatic mode (via interface) and manual mode (via method_name + params_types + return_type + codec).
        """
        return self._create_rpc_callable(
            rpc_type=RpcTypes.SERVER_STREAM.value,
            interface=interface,
            method_name=method_name,
            params_types=params_types,
            return_type=return_type,
            codec=codec,
            request_serializer=request_serializer,
            response_deserializer=response_deserializer,
            default_method_name="server_stream",
        )

    def bi_stream(
        self,
        interface: Optional[Callable] = None,
        method_name: Optional[str] = None,
        params_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        codec: Optional[str] = None,
        request_serializer: Optional[SerializingFunction] = None,
        response_deserializer: Optional[DeserializingFunction] = None,
    ) -> RpcCallable:
        """
        Create bidirectional streaming RPC call.

        Supports both automatic mode (via interface) and manual mode (via method_name + params_types + return_type + codec).
        """
        return self._create_rpc_callable(
            rpc_type=RpcTypes.BI_STREAM.value,
            interface=interface,
            method_name=method_name,
            params_types=params_types,
            return_type=return_type,
            codec=codec,
            request_serializer=request_serializer,
            response_deserializer=response_deserializer,
            default_method_name="bi_stream",
        )

    def _callable(self, method_descriptor: MethodDescriptor) -> RpcCallable:
        """
        Generate a proxy for the given method.
        :param method_descriptor: The method descriptor.
        :return: The proxy.
        :rtype: RpcCallable
        """
        # get invoker
        url = self._invoker.get_url()

        # clone url
        url = url.copy()
        url.parameters[common_constants.METHOD_KEY] = method_descriptor.get_method_name()
        # set method descriptor
        url.attributes[common_constants.METHOD_DESCRIPTOR_KEY] = method_descriptor

        # create proxy
        return self._callable_factory.get_callable(self._invoker, url)