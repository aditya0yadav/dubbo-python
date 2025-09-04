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

from typing import Any, List, Optional, Type
from dubbo.codec.json_codec import JsonTransportCodec

__all__ = ["JsonTransportCodecBridge", "JsonParameterEncoder", "JsonReturnDecoder"]


class JsonParameterEncoder:
    """
    Parameter encoder wrapper for JsonTransportCodec.
    """

    def __init__(self, codec: JsonTransportCodec):
        self._codec = codec

    def encode(self, arguments: tuple) -> bytes:
        """
        Encode method parameters.

        :param arguments: The method arguments to encode.
        :type arguments: tuple
        :return: Encoded parameter bytes.
        :rtype: bytes
        """
        return self._codec.encode_parameters(*arguments)


class JsonReturnDecoder:
    """
    Return value decoder wrapper for JsonTransportCodec.
    """

    def __init__(self, codec: JsonTransportCodec):
        self._codec = codec

    def decode(self, data: bytes) -> Any:
        """
        Decode method return value.

        :param data: The bytes to decode.
        :type data: bytes
        :return: Decoded return value.
        :rtype: Any
        """
        return self._codec.decode_return_value(data)


class JsonTransportCodecBridge:
    """
    Bridge class that adapts JsonTransportCodec to work with DubboSerializationService.

    This maintains compatibility with the existing extension loader system while
    using the clean new codec architecture internally.
    """

    def __init__(
        self,
        parameter_types: Optional[List[Type]] = None,
        return_type: Optional[Type] = None,
        maximum_depth: int = 100,
        strict_validation: bool = True,
        **kwargs,
    ):
        """
        Initialize the codec bridge.

        :param parameter_types: List of parameter types for the method.
        :param return_type: Return type for the method.
        :param maximum_depth: Maximum serialization depth.
        :param strict_validation: Whether to use strict validation.
        """
        self._codec = JsonTransportCodec(
            parameter_types=parameter_types,
            return_type=return_type,
            maximum_depth=maximum_depth,
            strict_validation=strict_validation,
            **kwargs,
        )

        # Create encoder and decoder instances
        self._encoder = JsonParameterEncoder(self._codec)
        self._decoder = JsonReturnDecoder(self._codec)

    def encoder(self) -> JsonParameterEncoder:
        """
        Get the parameter encoder instance.

        :return: The parameter encoder.
        :rtype: JsonParameterEncoder
        """
        return self._encoder

    def decoder(self) -> JsonReturnDecoder:
        """
        Get the return value decoder instance.

        :return: The return value decoder.
        :rtype: JsonReturnDecoder
        """
        return self._decoder

    # Direct access methods for convenience
    def encode_parameters(self, *arguments) -> bytes:
        """Direct parameter encoding."""
        return self._codec.encode_parameters(*arguments)

    def decode_return_value(self, data: bytes) -> Any:
        """Direct return value decoding."""
        return self._codec.decode_return_value(data)

    # Properties for access to internal codec if needed
    @property
    def codec(self) -> JsonTransportCodec:
        """Access to the underlying codec."""
        return self._codec
