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
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
import inspect
from typing import Optional, Callable, List, Type, Any, get_type_hints

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
            protocol = extensionLoader.get_extension(
                Protocol, self._reference.protocol
            )()

            registry_config = self._dubbo.registry_config

            self._protocol = (
                RegistryProtocol(registry_config, protocol)
                if registry_config
                else protocol
            )

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

    @classmethod
    def _infer_types_from_interface(cls, interface: Callable) -> tuple:
        """
        Infer method name, parameter types, and return type from a callable.
        """
        try:
            type_hints = get_type_hints(interface)
            sig = inspect.signature(interface)
            method_name = interface.__name__
            params = list(sig.parameters.values())

            # skip 'self' for bound methods
            if params and params[0].name == "self":
                params = params[1:]

            param_types = [type_hints.get(p.name, Any) for p in params]
            return_type = type_hints.get("return", Any)

            return method_name, param_types, return_type
        except Exception:
            return interface.__name__, [Any], Any

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
        if interface is None and method_name is None:
            raise ValueError("Either 'interface' or 'method_name' must be provided")

        # Start with explicit values
        m_name = method_name
        p_types = params_types
        r_type = return_type

        # Infer from interface if needed
        if interface:
            if p_types is None or r_type is None or m_name is None:
                inf_name, inf_params, inf_return = self._infer_types_from_interface(
                    interface
                )
                m_name = m_name or inf_name
                p_types = p_types or inf_params
                r_type = r_type or inf_return

        # Fallback to default
        m_name = m_name or default_method_name

        # Determine serializers
        if request_serializer and response_deserializer:
            req_ser = request_serializer
            res_deser = response_deserializer
        else:
            req_ser, res_deser = DubboTransportService.create_serialization_functions(
                codec or "json",  # fallback to json
                parameter_types=p_types,
                return_type=r_type,
            )

        # Create MethodDescriptor
        descriptor = MethodDescriptor(
            method_name=m_name,
            arg_serialization=(req_ser, None),
            return_serialization=(None, res_deser),
            rpc_type=rpc_type,
        )

        return self._callable(descriptor)

    def unary(self, **kwargs) -> RpcCallable:
        return self._create_rpc_callable(
            rpc_type=RpcTypes.UNARY.value, default_method_name="unary", **kwargs
        )

    def client_stream(self, **kwargs) -> RpcCallable:
        return self._create_rpc_callable(
            rpc_type=RpcTypes.CLIENT_STREAM.value,
            default_method_name="client_stream",
            **kwargs,
        )

    def server_stream(self, **kwargs) -> RpcCallable:
        return self._create_rpc_callable(
            rpc_type=RpcTypes.SERVER_STREAM.value,
            default_method_name="server_stream",
            **kwargs,
        )

    def bi_stream(self, **kwargs) -> RpcCallable:
        return self._create_rpc_callable(
            rpc_type=RpcTypes.BI_STREAM.value, default_method_name="bi_stream", **kwargs
        )

    def _callable(self, method_descriptor: MethodDescriptor) -> RpcCallable:
        """
        Generate a proxy for the given method.
        """
        url = self._invoker.get_url().copy()
        url.parameters[common_constants.METHOD_KEY] = (
            method_descriptor.get_method_name()
        )
        url.attributes[common_constants.METHOD_DESCRIPTOR_KEY] = method_descriptor
        return self._callable_factory.get_callable(self._invoker, url)
