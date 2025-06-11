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

from abc import ABC, abstractmethod
from typing import Any, Type, Optional, Callable
from .base_codec import Codec
from .codec_registry import CodecRegistry
from pydantic import BaseModel

class DubboCodec:
    _codec_instance: Optional[Codec] = None

    @staticmethod
    def init(codec_type: str = 'json', model_type: Optional[Type[BaseModel]] = None, **codec_kwargs):
        """Initialize codec with specified type and options"""
        if model_type is None:
            raise ValueError("model_type is required for all codecs")
        
        DubboCodec._codec_instance = CodecRegistry.get_codec(
            codec_type, 
            model_type=model_type, 
            **codec_kwargs
        )

    @staticmethod
    def get_instance() -> Codec:
        if DubboCodec._codec_instance is None:
            raise RuntimeError("DubboCodec is not initialized. Call DubboCodec.init(...) first.")
        return DubboCodec._codec_instance

    @staticmethod
    def encode(data: Any) -> bytes:
        return DubboCodec.get_instance().encode(data)

    @staticmethod
    def decode(data: bytes) -> Any:
        return DubboCodec.get_instance().decode(data)

    @staticmethod
    def get_serializer_deserializer(
        codec_type: str, 
        request_model: Type[BaseModel] = None, 
        response_model: Type[BaseModel] = None,
        **codec_kwargs
    ) -> tuple[Callable, Callable]:
        """Get serializer and deserializer functions for RPC"""
        
        request_codec = CodecRegistry.get_codec(codec_type, model_type=request_model, **codec_kwargs)
        
        response_codec = CodecRegistry.get_codec(codec_type, model_type=response_model, **codec_kwargs)
        
        def request_deserializer(data: bytes):
            return request_codec.decode(data)
        
        def response_serializer(response):
            return response_codec.encode(response)
        
        return request_deserializer, response_serializer
