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

import abc
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional, List, Tuple

__all__ = [
    "ParameterDescriptor",
    "MethodDescriptor",
    "TransportCodec",
    "SerializationEncoder",
    "SerializationDecoder",
]

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
    parameters: List[ParameterDescriptor]
    return_parameter: ParameterDescriptor
    documentation: Optional[str] = None


class TransportCodec(abc.ABC):
    """
    The transport codec interface.
    """

    @classmethod
    @abc.abstractmethod
    def get_transport_type(cls) -> str:
        """
        Get transport type of current codec
        :return: The transport type.
        :rtype: str
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_encoder(self) -> "SerializationEncoder":
        """
        Get encoder instance
        :return: The encoder.
        :rtype: SerializationEncoder
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_decoder(self) -> "SerializationDecoder":
        """
        Get decoder instance
        :return: The decoder.
        :rtype: SerializationDecoder
        """
        raise NotImplementedError()


class SerializationEncoder(abc.ABC):
    """
    The serialization encoder interface.
    """

    @abc.abstractmethod
    def encode(self, arguments: Tuple[Any, ...]) -> bytes:
        """
        Encode arguments to bytes.
        :param arguments: The arguments to encode.
        :type arguments: Tuple[Any, ...]
        :return: The encoded bytes.
        :rtype: bytes
        """
        raise NotImplementedError()


class SerializationDecoder(abc.ABC):
    """
    The serialization decoder interface.
    """

    @abc.abstractmethod
    def decode(self, data: bytes) -> Any:
        """
        Decode bytes to object.
        :param data: The data to decode.
        :type data: bytes
        :return: The decoded object.
        :rtype: Any
        """
        raise NotImplementedError()
