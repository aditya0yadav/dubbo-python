from abc import ABC, abstractmethod
from typing import Any, Union

class Codec(ABC):
    """Base codec interface for encoding/decoding data"""
    
    @abstractmethod
    def encode(self, data: Any) -> bytes:
        """Encode Python object to bytes"""
        pass
    
    @abstractmethod
    def decode(self, data: bytes) -> Any:
        """Decode bytes to Python object"""
        pass
