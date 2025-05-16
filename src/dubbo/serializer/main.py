from abc import ABC, abstractmethod
from _pydantic import JsonProcessor
import datetime
from pydantic import BaseModel, Field
from typing import List, Optional

class DataInfo:
    """Class that carries information about data processing requirements"""
    
    VALID_FORMATS = ["json", "protobuf", "hessian"]
    
    def __init__(self, format, **kwargs):
        """Initialize with format and additional parameters
        
        Args:
            format: The data format (json, protobuf, hessian)
            **kwargs: Additional parameters for processing
        """
        self.format = format
        self.additional_params = kwargs
        self.is_valid = self._validate_format()
        
    def _validate_format(self):
        """Validate if the format is supported"""
        if self.format in self.VALID_FORMATS:
            return True
        else:
            print(f"Please check the format you specified. It can only be: {', '.join(self.VALID_FORMATS)}")
            return False
            
    def get_format(self):
        """Return the specified format"""
        return self.format
        
    def get_param(self, key, default=None):
        """Get an additional parameter by key"""
        return self.additional_params.get(key, default)
        
    def is_valid_format(self):
        """Check if the format is valid"""
        return self.is_valid


class Processor(ABC):
    """Abstract base class for all format processors"""
    
    @abstractmethod
    def process(self, data_info):
        """Process data according to the provided DataInfo"""
        pass



class ProtobufProcessor(Processor):
    """Protobuf format processor"""
    
    def process(self, data_info):
        print("Processing with Protobuf format")
        version = data_info.get_param('version', '3')
        print(f"Using Protobuf version: {version}")


class HessianProcessor(Processor):
    """Hessian format processor"""
    
    def process(self, data_info):
        print("Processing with Hessian format")
        encoding = data_info.get_param('encoding', 'utf-8')
        print(f"Using encoding: {encoding}")


class DataProcessorFactory:
    """Factory that creates appropriate processors based on DataInfo"""
    
    @staticmethod
    def create_processor(data_info):
        """Create and return the appropriate processor based on format
        
        Args:
            data_info: DataInfo instance containing processing requirements
            
        Returns:
            Processor: The appropriate processor for the format
            
        Raises:
            ValueError: If format is invalid or not supported
        """
        if not data_info.is_valid_format():
            raise ValueError("Invalid format specified in DataInfo")
            
        format_type = data_info.get_format()
        
        processors = {
            "json": JsonProcessor,
            "protobuf": ProtobufProcessor,
            "hessian": HessianProcessor
        }
        
        processor_class = processors.get(format_type)
        if not processor_class:
            raise ValueError(f"No processor available for format: {format_type}")
        
        return processor_class()


class DataProcessorWorker:
    """Worker class that uses DataInfo to process data"""
    
    def __init__(self, data_info):
        """Initialize with DataInfo
        
        Args:
            data_info: DataInfo instance containing processing requirements
            
        Raises:
            TypeError: If data_info is not a DataInfo instance
        """
        if not isinstance(data_info, DataInfo):
            raise TypeError("data_info must be an instance of DataInfo")
            
        self.data_info = data_info
        
    def process(self):
        """Process data according to the information in DataInfo"""
        if not self.data_info.is_valid_format():
            print("Cannot process: invalid format")
            return False
            
        processor = DataProcessorFactory.create_processor(self.data_info)
        processor.process(self.data_info)
        return True
    
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


def test_json_processor_with_data_info():
    print("\n===== Testing JsonProcessor with DataInfo =====")
    
    # Create DataInfo for JSON processing
    data_info = DataInfo(format="json", model_class=User)
    
    # Verify DataInfo is valid
    print(f"DataInfo format: {data_info.get_format()}")
    print(f"Is valid format: {data_info.is_valid_format()}")
    print(f"Model class: {data_info.get_param('model_class')}")
    
    # Create a worker with the DataInfo
    worker = DataProcessorWorker(data_info)
    
    # Create test user data
    user = User(
        id=101,
        name="Charlie Brown",
        email="charlie@example.com",
        age=25,
        addresses=[
            Address(street="555 Main St", city="Chicago", zip_code="60601")
        ]
    )
    
    print(f"\nOriginal User object: {user}")
    
    # Get processor from factory and process the data
    processor = DataProcessorFactory.create_processor(data_info)
    
    # Since the JsonProcessor from previous module expects either a model instance or bytes,
    # we need to adapt it to work with our DataProcessorWorker pattern
    
    # Serialize user object to bytes
    serialized_data = processor.process(user)
    print(f"\nSerialized data (bytes): {serialized_data}")
    print(f"Decoded JSON: {serialized_data.decode('utf-8')}")
    
    # Deserialize back to User object
    deserialized_user = processor.process(serialized_data)
    print(f"\nDeserialized User object: {deserialized_user}")
    
    # Verify addresses were properly deserialized
    if deserialized_user.addresses:
        print(f"\nDeserialized address: {deserialized_user.addresses[0]}")


def test_invalid_format():
    print("\n===== Testing Invalid Format =====")
    
    # Create DataInfo with invalid format
    data_info = DataInfo(format="xml", model_class=User)
    
    print(f"DataInfo format: {data_info.get_format()}")
    print(f"Is valid format: {data_info.is_valid_format()}")
    
    # Try to create a worker and process
    worker = DataProcessorWorker(data_info)
    result = worker.process()
    
    print(f"Processing result: {result}")


def test_all_formats():
    print("\n===== Testing All Formats =====")
    
    formats = ["json", "protobuf", "hessian"]
    
    for format_name in formats:
        print(f"\nTesting format: {format_name}")
        
        # Create DataInfo with the format
        data_info = DataInfo(
            format=format_name,
            model_class=User if format_name == "json" else None,
            version="3.2" if format_name == "protobuf" else None,
            encoding="utf-16" if format_name == "hessian" else None
        )
        
        # Special handling for JsonProcessor
        if format_name == "json":
            print("Testing JsonProcessor with a test user instance")
            processor = DataProcessorFactory.create_processor(data_info)
            
            # Create a test user
            test_user = User(
                id=999,
                name="Test User",
                email="test@example.com"
            )
            
            # Process the user object instead of the DataInfo
            serialized = processor.process(test_user)
            print(f"Successfully serialized: {len(serialized)} bytes")
            
            deserialized = processor.process(serialized)
            print(f"Successfully deserialized to: {deserialized.name}")
            
            result = True
        else:
            # For other processors, use the standard WorkerProcessor approach
            worker = DataProcessorWorker(data_info)
            result = worker.process()
        
        print(f"Processing result: {result}")



# Fix JsonProcessor integration with DataProcessorFactory
# We need to modify the factory to pass model_class to JsonProcessor
def patch_json_processor_factory():
    original_create_processor = DataProcessorFactory.create_processor
    
    def patched_create_processor(data_info):
        format_type = data_info.get_format()
        
        if format_type == "json":
            model_class = data_info.get_param('model_class')
            if not model_class:
                raise ValueError("model_class parameter is required for JsonProcessor")
            return JsonProcessor(model_class)
        else:
            return original_create_processor(data_info)
    
    DataProcessorFactory.create_processor = staticmethod(patched_create_processor)


if __name__ == "__main__":
    from _pydantic import JsonProcessor
    # Apply patch to make JsonProcessor work with the factory
    patch_json_processor_factory()
    
    # Run the tests
    test_json_processor_with_data_info()
    test_invalid_format()
    test_all_formats()