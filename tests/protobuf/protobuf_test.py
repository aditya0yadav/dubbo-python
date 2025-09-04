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

from generated.protobuf_test import GreeterReply, GreeterRequest

from dubbo.codec.protobuf_codec import ProtobufTransportCodec, ProtobufTransportDecoder


def test_protobuf_roundtrip_message():
    codec = ProtobufTransportCodec(parameter_type=GreeterRequest, return_type=GreeterReply)

    # Create a request
    req = GreeterRequest(name="Alice")

    # Encode
    encoded = codec.encode_parameter(req)
    assert isinstance(encoded, bytes)

    # Fake a server reply
    reply = GreeterReply(message="Hello Alice")
    reply_bytes = bytes(reply)

    # Decode return value
    decoded = codec.decode_return_value(reply_bytes)
    assert isinstance(decoded, GreeterReply)
    assert decoded.message == "Hello Alice"


def test_protobuf_from_dict():
    codec = ProtobufTransportCodec(parameter_type=GreeterRequest, return_type=GreeterReply)

    # Dict instead of message instance
    encoded = codec.encode_parameter({"name": "Bob"})
    assert isinstance(encoded, bytes)

    # To decode back to the parameter type, we need a decoder configured for GreeterRequest
    param_decoder = ProtobufTransportDecoder(target_type=GreeterRequest)
    req = param_decoder.decode(encoded)
    assert isinstance(req, GreeterRequest)
    assert req.name == "Bob"


def test_protobuf_primitive_fallback():
    codec = ProtobufTransportCodec(parameter_type=str, return_type=str)

    encoded = codec.encode_parameter("simple string")
    assert isinstance(encoded, bytes)

    # Decode back
    decoded = codec.decode_return_value(encoded)
    assert isinstance(decoded, str)
    assert decoded == "simple string"
