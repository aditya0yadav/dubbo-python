from abc import ABC, abstractmethod
from typing import Any, Type, Protocol, Callable, TypeVar, Generic, Optional, List
import dubbo
from functools import wraps
from pydantic import BaseModel, Field

# Define proper Protocols with clearer intent
class SerializingFunction(Protocol):
    def __call__(self, obj: Any) -> bytes: ...

class DeserializingFunction(Protocol):
    def __call__(self, data: bytes) -> Any: ...

# Define generic type variable for better type safety
T = TypeVar('T')

class Processor(ABC):
    """Abstract base class for all format processors"""
    
    @abstractmethod
    def process(self, data_info: Any) -> Any:
        """Process data according to the provided DataInfo"""
        pass

class JsonProcessor(Processor, Generic[T]):
    """JSON format processor for Pydantic models
    
    This processor handles serialization and deserialization of Pydantic models
    to and from JSON format.
    """
    
    def __init__(self, model_class: Type[T]):
        """Initialize with the model class to use for serialization/deserialization
        
        Args:
            model_class: The Pydantic model class to use
        """
        self.model_class = model_class
    
    def process(self, data_info: Any) -> Any:
        """Process data according to the processor's rules
        
        This implementation serves as a router to serialization or deserialization
        based on the input type.
        
        Args:
            data_info: Either bytes to deserialize or a model instance to serialize
            
        Returns:
            Either bytes (after serialization) or a model instance (after deserialization)
        """
        if isinstance(data_info, bytes):
            return self.deserialize(data_info)
        else:
            return self.serialize(data_info)
    
    def serialize(self, obj: Any) -> bytes:
        """Serialize a Pydantic model or dict to JSON bytes
        
        Args:
            obj: Either a model instance or a dict to be converted to a model
            
        Returns:
            UTF-8 encoded JSON bytes
        """
        if isinstance(obj, dict):
            obj = self.model_class(**obj)
        elif not isinstance(obj, self.model_class):
            raise TypeError(f"Expected {self.model_class.__name__} or dict, got {type(obj).__name__}")
        
        return obj.model_dump_json().encode('utf-8')
    
    def deserialize(self, data: bytes) -> T:
        """Deserialize JSON bytes to a Pydantic model instance
        
        Args:
            data: UTF-8 encoded JSON bytes
            
        Returns:
            An instance of the model class
        """
        return self.model_class.model_validate_json(data.decode('utf-8'))

    @classmethod
    def create_serializer(cls, model_class: Type[T]) -> SerializingFunction:
        """Create a standalone serializing function for the given model class
        
        Args:
            model_class: The Pydantic model class to serialize to
            
        Returns:
            A function that serializes to bytes
        """
        processor = cls(model_class)
        return processor.serialize
    
    @classmethod
    def create_deserializer(cls, model_class: Type[T]) -> DeserializingFunction:
        """Create a standalone deserializing function for the given model class
        
        Args:
            model_class: The Pydantic model class to deserialize to
            
        Returns:
            A function that deserializes from bytes
        """
        processor = cls(model_class)
        return processor.deserialize

# Example usage:
# 
# class User(BaseModel):
#     name: str
#     age: int
#
# # Create a processor instance
# json_processor = JsonProcessor(User)
#
# # Serialize
# user = User(name="John", age=30)
# serialized_data = json_processor.process(user)
#
# # Deserialize
# user_obj = json_processor.process(serialized_data)
#
# # Or use the standalone functions
# serialize_user = JsonProcessor.create_serializer(User)
# deserialize_user = JsonProcessor.create_deserializer(User)
#
# bytes_data = serialize_user(user)
# user_again = deserialize_user(bytes_data)

import datetime
import json

# Import the JsonProcessor from your module

# Define some Pydantic models for testing
class Address(BaseModel):
    street: str
    city: str
    zip_code: str
    country: str = "USA"


class User(BaseModel):
    id: int
    name: str
    email: str
    age: Optional[int] = None
    is_active: bool = True
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    addresses: List[Address] = []


# Test 1: Basic serialization and deserialization
def test_basic_serialization():
    print("===== Test 1: Basic Serialization and Deserialization =====")
    
    # Create a processor for the User model
    processor = JsonProcessor(User)
    
    # Create a user instance
    user = User(
        id=1,
        name="John Doe",
        email="john@example.com",
        age=30
    )
    
    print(f"Original User object: {user}")
    
    # Serialize to bytes
    serialized_data = processor.process(user)
    print(f"\nSerialized data (bytes): {serialized_data}")
    print(f"Decoded JSON: {serialized_data.decode('utf-8')}")
    
    # Deserialize back to User
    deserialized_user = processor.process(serialized_data)
    print(f"\nDeserialized User object: {deserialized_user}")
    
    # Verify fields match
    print(f"\nVerification:")
    print(f"Original ID: {user.id}, Deserialized ID: {deserialized_user.id}")
    print(f"Original Name: {user.name}, Deserialized Name: {deserialized_user.name}")
    print(f"Original Email: {user.email}, Deserialized Email: {deserialized_user.email}")
    print(f"Original Age: {user.age}, Deserialized Age: {deserialized_user.age}")


# Test 2: Serializing a dict instead of a model instance
def test_dict_serialization():
    print("\n\n===== Test 2: Dict Serialization =====")
    
    # Create a processor for the User model
    processor = JsonProcessor(User)
    
    # Create a user as a dict
    user_dict = {
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com",
        "age": 28,
        "is_active": True
    }
    
    print(f"Original dict: {user_dict}")
    
    # Serialize to bytes
    serialized_data = processor.process(user_dict)
    print(f"\nSerialized data (bytes): {serialized_data}")
    print(f"Decoded JSON: {serialized_data.decode('utf-8')}")
    
    # Deserialize back to User
    deserialized_user = processor.process(serialized_data)
    print(f"\nDeserialized User object: {deserialized_user}")


# Test 3: Complex model with nested objects
def test_complex_model():
    print("\n\n===== Test 3: Complex Model with Nested Objects =====")
    
    # Create a processor for the User model
    processor = JsonProcessor(User)
    
    # Create a user with addresses
    user = User(
        id=3,
        name="Bob Johnson",
        email="bob@example.com",
        addresses=[
            Address(street="123 Main St", city="New York", zip_code="10001"),
            Address(street="456 Park Ave", city="Boston", zip_code="02101")
        ]
    )
    
    print(f"Original User with addresses: {user}")
    
    # Serialize to bytes
    serialized_data = processor.process(user)
    print(f"\nSerialized data (decoded):\n{json.dumps(json.loads(serialized_data.decode('utf-8')), indent=2)}")
    
    # Deserialize back to User
    deserialized_user = processor.process(serialized_data)
    print(f"\nDeserialized User object: {deserialized_user}")
    print(f"\nDeserialized addresses:")
    for i, addr in enumerate(deserialized_user.addresses):
        print(f"  Address {i+1}: {addr}")


# Test 4: Using standalone serializer and deserializer functions
def test_standalone_functions():
    print("\n\n===== Test 4: Standalone Serializer and Deserializer Functions =====")
    
    # Create standalone functions
    serialize_user = JsonProcessor.create_serializer(User)
    deserialize_user = JsonProcessor.create_deserializer(User)
    
    # Create a user instance
    user = User(
        id=4,
        name="Alice Williams",
        email="alice@example.com",
        age=35
    )
    
    print(f"Original User object: {user}")
    
    # Serialize using standalone function
    serialized_data = serialize_user(user)
    print(f"\nSerialized data (bytes): {serialized_data}")
    print(f"Decoded JSON: {serialized_data.decode('utf-8')}")
    
    # Deserialize using standalone function
    deserialized_user = deserialize_user(serialized_data)
    print(f"\nDeserialized User object: {deserialized_user}")


# Run all tests
if __name__ == "__main__":
    test_basic_serialization()
    test_dict_serialization()
    test_complex_model()
    test_standalone_functions()