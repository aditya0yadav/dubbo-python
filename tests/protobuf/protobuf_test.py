import pytest
from dubbo.codec.protobuf_codec import ProtobufTransportCodec
from generated.protobuf_test import GreeterReply, GreeterRequest


def test_protobuf_roundtrip_message():
    codec = ProtobufTransportCodec(
        parameter_type=GreeterRequest,
        return_type=GreeterReply
    )

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
    codec = ProtobufTransportCodec(
        parameter_type=GreeterRequest,
        return_type=GreeterReply
    )

    # Dict instead of message instance
    encoded = codec.encode_parameter({"name": "Bob"})
    assert isinstance(encoded, bytes)

    # Decode back to message
    req = codec._decoder.decode(encoded)  # simulate server echo
    assert isinstance(req, GreeterRequest)
    assert req.name == "Bob"


def test_protobuf_primitive_fallback():
    codec = ProtobufTransportCodec(
        parameter_type=str,
        return_type=str
    )

    encoded = codec.encode_parameter("simple string")
    assert isinstance(encoded, bytes)

    # Decode back
    decoded = codec.decode_return_value(encoded)
    assert isinstance(decoded, str)
    assert decoded == "simple string"
