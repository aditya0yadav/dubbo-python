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
from typing import Any, Type, Protocol, Callable, TypeVar, Generic
from functools import wraps
from pydantic import BaseModel
from dubbo.classes import Codec  # Fixed import path
import orjson


class EncodingFunction(Protocol):
    def __call__(self, obj: Any) -> bytes: ...


class DecodingFunction(Protocol):
    def __call__(self, data: bytes) -> Any: ...


ModelT = TypeVar('ModelT', bound=BaseModel)


class JsonCodec(Codec, Generic[ModelT]):
    """JSON codec for Pydantic models using orjson for performance"""
    
    def __init__(self, model_type: Type[ModelT], **kwargs):
        super().__init__(model_type=model_type, **kwargs)  # Call parent constructor
        self.model_type = model_type

    def encode(self, data: Any) -> bytes:
        """Encode data to JSON bytes"""
        if isinstance(data, dict):
            data = self.model_type(**data)
        elif not isinstance(data, self.model_type):
            raise TypeError(f"Expected {self.model_type.__name__} or dict, got {type(data).__name__}")
        return orjson.dumps(data.model_dump())

    def decode(self, data: bytes) -> ModelT:
        """Decode JSON bytes to Pydantic model"""
        json_data = orjson.loads(data)
        return self.model_type(**json_data)