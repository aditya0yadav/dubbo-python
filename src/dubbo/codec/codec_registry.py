from .base_codec import Codec
from dubbo.codec.json_codec import JsonCodec
from typing import Type, Any, Callable
from pydantic import BaseModel

class CodecRegistry:
    """Registry for managing different codec types"""

    _codecs = {
        'json': JsonCodec
    }

    @classmethod
    def get_codec(cls, codec_type: str, **kwargs) -> Codec:
        """Return codec instance"""
        if codec_type not in cls._codecs:
            raise ValueError(f"Unsupported codec type: {codec_type}. Available: {list(cls._codecs.keys())}")
        
        codec_class = cls._codecs[codec_type]
        return codec_class(**kwargs)

    @classmethod
    def get_encoder_decoder(
        cls,
        codec_type: str,
        model_type: Type[BaseModel],
        **kwargs
    ) -> tuple[Callable[[Any], bytes], Callable[[bytes], Any]]:
        """Return encoder and decoder functions"""
        codec_instance = cls.get_codec(codec_type, model_type=model_type, **kwargs)
        return codec_instance.encode, codec_instance.decode

    @classmethod
    def register_codec(cls, name: str, codec_class: type):
        """Register a new codec type"""
        cls._codecs[name] = codec_class

    @classmethod
    def list_available_codecs(cls) -> list[str]:
        """List all available codec types"""
        return list(cls._codecs.keys())
