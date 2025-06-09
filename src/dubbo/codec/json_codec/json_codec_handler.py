from abc import ABC, abstractmethod
from typing import Any, Type, Protocol, Callable, TypeVar, Generic
import dubbo
from functools import wraps
from pydantic import BaseModel
from typing import Any, Type, TypeVar, Generic
from pydantic import BaseModel
from dubbo.codec.base_codec import Codec
import orjson

class EncodingFunction(Protocol):
    def __call__(self, obj: Any) -> bytes: ...


class DecodingFunction(Protocol):
    def __call__(self, data: bytes) -> Any: ...

ModelT = TypeVar('ModelT', bound=BaseModel)

class JsonCodec(Codec, Generic[ModelT]):
    """JSON codec for Pydantic models using orjson for performance"""
    
    def __init__(self, model_type: Type[ModelT]):
        self.model_type = model_type

    def encode(self, data: Any) -> bytes:
        if isinstance(data, dict):
            data = self.model_type(**data)
        elif not isinstance(data, self.model_type):
            raise TypeError(f"Expected {self.model_type.__name__} or dict, got {type(data).__name__}")
        return orjson.dumps(data.model_dump())

    def decode(self, data: bytes) -> ModelT:
        json_data = orjson.loads(data)
        return self.model_type(**json_data)
