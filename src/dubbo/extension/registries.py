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

import importlib
from dataclasses import dataclass
from typing import Any

from dubbo.classes import Codec, SingletonBase

# Import all the required interface classes
from dubbo.cluster import LoadBalance
from dubbo.compression import Compressor, Decompressor
from dubbo.extension import registries as registries_module
from dubbo.protocol import Protocol
from dubbo.registry import RegistryFactory
from dubbo.remoting import Transporter


class ExtensionError(Exception):
    """
    Extension error.
    """

    def __init__(self, message: str):
        """
        Initialize the extension error.
        :param message: The error message.
        :type message: str
        """
        super().__init__(message)


class ExtensionLoader(SingletonBase):
    """
    Singleton class for loading extension implementations.
    """

    def __init__(self):
        """
        Initialize the extension loader.

        Load all the registries from the registries module.
        """
        if not hasattr(self, "_initialized"):  # Ensure __init__ runs only once
            self._registries = {}
            for name in registries_module.registries:
                registry = getattr(registries_module, name)
                self._registries[registry.interface] = registry.impls
            self._initialized = True

    def get_extension(self, interface: Any, impl_name: str) -> Any:
        """
        Get the extension implementation for the interface.

        :param interface: Interface class.
        :type interface: Any
        :param impl_name: Implementation name.
        :type impl_name: str
        :return: Extension implementation class.
        :rtype: Any
        :raises ExtensionError: If the interface or implementation is not found.
        """
        # Get the registry for the interface
        impls = self._registries.get(interface)
        print("value is ", impls, interface)
        if not impls:
            raise ExtensionError(f"Interface '{interface.__name__}' is not supported.")

        # Get the full name of the implementation
        full_name = impls.get(impl_name)
        if not full_name:
            raise ExtensionError(f"Implementation '{impl_name}' for interface '{interface.__name__}' is not exist.")

        try:
            # Split the full name into module and class
            module_name, class_name = full_name.rsplit(".", 1)

            # Load the module and get the class
            module = importlib.import_module(module_name)
            subclass = getattr(module, class_name)

            # Return the subclass
            return subclass
        except Exception as e:
            raise ExtensionError(
                f"Failed to load extension '{impl_name}' for interface '{interface.__name__}'. \nDetail: {e}"
            )


@dataclass
class ExtendedRegistry:
    """
    A dataclass to represent an extended registry.

    :param interface: The interface of the registry.
    :type interface: Any
    :param impls: The implementations of the registry.
    :type impls: Dict[str, Any]
    """

    interface: Any
    impls: dict[str, Any]


# All Extension Registries - FIXED: Added codecRegistry to the list
registries = [
    "registryFactoryRegistry",
    "loadBalanceRegistry",
    "protocolRegistry",
    "compressorRegistry",
    "decompressorRegistry",
    "transporterRegistry",
    "codecRegistry",
]

# RegistryFactory registry
registryFactoryRegistry = ExtendedRegistry(
    interface=RegistryFactory,
    impls={
        "zookeeper": "dubbo.registry.zookeeper.zk_registry.ZookeeperRegistryFactory",
    },
)

# LoadBalance registry
loadBalanceRegistry = ExtendedRegistry(
    interface=LoadBalance,
    impls={
        "random": "dubbo.cluster.loadbalances.RandomLoadBalance",
        "cpu": "dubbo.cluster.loadbalances.CpuLoadBalance",
    },
)

# Protocol registry
protocolRegistry = ExtendedRegistry(
    interface=Protocol,
    impls={
        "tri": "dubbo.protocol.triple.protocol.TripleProtocol",
    },
)

# Compressor registry
compressorRegistry = ExtendedRegistry(
    interface=Compressor,
    impls={
        "identity": "dubbo.compression.Identity",
        "gzip": "dubbo.compression.Gzip",
        "bzip2": "dubbo.compression.Bzip2",
    },
)

# Decompressor registry
decompressorRegistry = ExtendedRegistry(
    interface=Decompressor,
    impls={
        "identity": "dubbo.compression.Identity",
        "gzip": "dubbo.compression.Gzip",
        "bzip2": "dubbo.compression.Bzip2",
    },
)

# Transporter registry
transporterRegistry = ExtendedRegistry(
    interface=Transporter,
    impls={
        "aio": "dubbo.remoting.aio.aio_transporter.AioTransporter",
    },
)

# Codec Registry
codecRegistry = ExtendedRegistry(
    interface=Codec,
    impls={
        "json": "dubbo.codec.json_codec.JsonTransportCodec",
        "protobuf": "dubbo.codec.protobuf_codec.ProtobufTransportCodec",
    },
)
