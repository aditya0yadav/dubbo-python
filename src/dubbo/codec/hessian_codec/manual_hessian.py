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

import io
import struct
from typing import Any, Dict, Type
from datetime import datetime, date, time
from decimal import Decimal
import uuid


class Hessian2Output:
    
    TYPE_MARKERS = {
        'set': 0xF0,
        'frozenset': 0xF1, 
        'tuple': 0xF2,
        'decimal': 0xF3,
        'uuid': 0xF4,
        'date': 0xF5,
        'time': 0xF6,
        'complex': 0xF7,
        'range': 0xF8,
        'bytearray': 0xF9,
        'custom_object': 0xFA
    }
    
    def __init__(self, stream: io.BytesIO):
        self.stream = stream

    def write_object(self, obj: Any):
        """Write any Python object to Hessian format."""
        if obj is None:
            self._write_null()
        elif isinstance(obj, bool):
            self._write_bool(obj)
        elif isinstance(obj, int):
            self._write_int(obj)
        elif isinstance(obj, float):
            self._write_float(obj)
        elif isinstance(obj, str):
            self._write_string(obj)
        elif isinstance(obj, bytes):
            self._write_bytes(obj)
        elif isinstance(obj, bytearray):
            self._write_bytearray(obj)
        elif isinstance(obj, list):
            self._write_list(obj)
        elif isinstance(obj, tuple):
            self._write_tuple(obj)
        elif isinstance(obj, dict):
            self._write_dict(obj)
        elif isinstance(obj, set):
            self._write_set(obj)
        elif isinstance(obj, frozenset):
            self._write_frozenset(obj)
        elif isinstance(obj, Decimal):
            self._write_decimal(obj)
        elif isinstance(obj, uuid.UUID):
            self._write_uuid(obj)
        elif isinstance(obj, datetime):
            self._write_datetime(obj)
        elif isinstance(obj, date):
            self._write_date(obj)
        elif isinstance(obj, time):
            self._write_time(obj)
        elif isinstance(obj, complex):
            self._write_complex(obj)
        elif isinstance(obj, range):
            self._write_range(obj)
        else:
            self._write_custom_object(obj)

    def _write_null(self):
        self.stream.write(b'N')

    def _write_bool(self, obj: bool):
        self.stream.write(b'T' if obj else b'F')

    def _write_int(self, obj: int):
        if -16 <= obj <= 47:
            self.stream.write(bytes([0x90 + obj]))
        elif -2048 <= obj <= 2047:
            self.stream.write(bytes([0xc8 + (obj >> 8), obj & 0xff]))
        else:
            self.stream.write(b'I' + struct.pack(">i", obj))

    def _write_float(self, obj: float):
        self.stream.write(b'D' + struct.pack(">d", obj))

    def _write_string(self, obj: str):
        encoded = obj.encode("utf-8")
        length = len(encoded)
        if length <= 31:
            self.stream.write(bytes([length]) + encoded)
        else:
            self.stream.write(b'S' + struct.pack(">H", length) + encoded)

    def _write_bytes(self, obj: bytes):
        length = len(obj)
        self.stream.write(b'B' + struct.pack(">H", length) + obj)

    def _write_bytearray(self, obj: bytearray):
        self.stream.write(bytes([self.TYPE_MARKERS['bytearray']]))
        self._write_bytes(bytes(obj))

    def _write_list(self, obj: list):
        self.stream.write(b'V')
        for item in obj:
            self.write_object(item)
        self.stream.write(b'Z')

    def _write_tuple(self, obj: tuple):
        self.stream.write(bytes([self.TYPE_MARKERS['tuple']]))
        self.stream.write(struct.pack(">I", len(obj)))
        for item in obj:
            self.write_object(item)

    def _write_dict(self, obj: dict):
        self.stream.write(b'M')
        for k, v in obj.items():
            self.write_object(k)
            self.write_object(v)
        self.stream.write(b'Z')

    def _write_set(self, obj: set):
        self.stream.write(bytes([self.TYPE_MARKERS['set']]))
        self.stream.write(struct.pack(">I", len(obj)))
        for item in obj:
            self.write_object(item)

    def _write_frozenset(self, obj: frozenset):
        self.stream.write(bytes([self.TYPE_MARKERS['frozenset']]))
        self.stream.write(struct.pack(">I", len(obj)))
        for item in obj:
            self.write_object(item)

    def _write_decimal(self, obj: Decimal):
        self.stream.write(bytes([self.TYPE_MARKERS['decimal']]))
        self._write_string(str(obj))

    def _write_uuid(self, obj: uuid.UUID):
        self.stream.write(bytes([self.TYPE_MARKERS['uuid']]))
        self._write_bytes(obj.bytes)

    def _write_datetime(self, obj: datetime):
        # Use standard Hessian date format
        timestamp = int(obj.timestamp() * 1000)
        self.stream.write(b'd' + struct.pack(">q", timestamp))

    def _write_date(self, obj: date):
        self.stream.write(bytes([self.TYPE_MARKERS['date']]))
        # Store as year, month, day
        self.stream.write(struct.pack(">HBB", obj.year, obj.month, obj.day))

    def _write_time(self, obj: time):
        self.stream.write(bytes([self.TYPE_MARKERS['time']]))
        # Store as hour, minute, second, microsecond
        self.stream.write(struct.pack(">BBBI", obj.hour, obj.minute, obj.second, obj.microsecond))

    def _write_complex(self, obj: complex):
        self.stream.write(bytes([self.TYPE_MARKERS['complex']]))
        self.stream.write(struct.pack(">dd", obj.real, obj.imag))

    def _write_range(self, obj: range):
        self.stream.write(bytes([self.TYPE_MARKERS['range']]))
        self.stream.write(struct.pack(">iii", obj.start, obj.stop, obj.step))

    def _write_custom_object(self, obj: Any):
        """Serialize custom objects using their __dict__."""
        self.stream.write(bytes([self.TYPE_MARKERS['custom_object']]))
        
        # Write class name
        class_name = f"{obj.__class__.__module__}.{obj.__class__.__name__}"
        self._write_string(class_name)
        
        # Write object data
        if hasattr(obj, '__dict__'):
            self._write_dict(obj.__dict__)
        else:
            # Fallback: try to convert to string
            self._write_string(str(obj))


class Hessian2Input:
    """Enhanced manual Hessian deserializer supporting additional Python types."""
    
    def __init__(self, stream: io.BytesIO):
        self.stream = stream
        self.type_registry: Dict[str, Type] = {}

    def register_type(self, class_path: str, cls: Type):
        """Register a custom type for deserialization."""
        self.type_registry[class_path] = cls

    def _read(self, n: int) -> bytes:
        data = self.stream.read(n)
        if len(data) != n:
            raise ValueError(f"Expected {n} bytes, got {len(data)}")
        return data

    def _read_byte(self) -> int:
        data = self.stream.read(1)
        if not data:
            raise ValueError("Unexpected end of stream")
        return data[0]

    def read_object(self) -> Any:
        """Read any Python object from Hessian format."""
        tag = self._read_byte()

        # Standard Hessian types
        if tag == ord('N'):
            return None
        elif tag == ord('T'):
            return True
        elif tag == ord('F'):
            return False
        elif tag == ord('I'):
            return struct.unpack(">i", self._read(4))[0]
        elif 0x90 <= tag <= 0xbf:
            return tag - 0x90
        elif 0xc0 <= tag <= 0xcf:
            return ((tag - 0xc8) << 8) + self._read_byte()
        elif 0xd0 <= tag <= 0xd7:
            return ((tag - 0xd4) << 16) + (self._read_byte() << 8) + self._read_byte()
        elif tag == ord('D'):
            return struct.unpack(">d", self._read(8))[0]
        elif tag == ord('S'):
            length = struct.unpack(">H", self._read(2))[0]
            return self._read(length).decode("utf-8")
        elif 0x00 <= tag <= 0x1f:
            return self._read(tag).decode("utf-8")
        elif tag == ord('B'):
            length = struct.unpack(">H", self._read(2))[0]
            return self._read(length)
        elif tag == ord('V'):
            return self._read_list()
        elif tag == ord('M'):
            return self._read_dict()
        elif tag == ord('d'):
            # Standard datetime
            timestamp_ms = struct.unpack(">q", self._read(8))[0]
            return datetime.fromtimestamp(timestamp_ms / 1000)
        
        # Custom types
        elif tag == Hessian2Output.TYPE_MARKERS['set']:
            return self._read_set()
        elif tag == Hessian2Output.TYPE_MARKERS['frozenset']:
            return self._read_frozenset()
        elif tag == Hessian2Output.TYPE_MARKERS['tuple']:
            return self._read_tuple()
        elif tag == Hessian2Output.TYPE_MARKERS['decimal']:
            return self._read_decimal()
        elif tag == Hessian2Output.TYPE_MARKERS['uuid']:
            return self._read_uuid()
        elif tag == Hessian2Output.TYPE_MARKERS['date']:
            return self._read_date()
        elif tag == Hessian2Output.TYPE_MARKERS['time']:
            return self._read_time()
        elif tag == Hessian2Output.TYPE_MARKERS['complex']:
            return self._read_complex()
        elif tag == Hessian2Output.TYPE_MARKERS['range']:
            return self._read_range()
        elif tag == Hessian2Output.TYPE_MARKERS['bytearray']:
            return self._read_bytearray()
        elif tag == Hessian2Output.TYPE_MARKERS['custom_object']:
            return self._read_custom_object()
        else:
            raise ValueError(f"Unknown Hessian tag: {hex(tag)}")

    def _read_list(self) -> list:
        arr = []
        while True:
            peek = self.stream.read(1)
            if peek == b'Z':
                break
            self.stream.seek(-1, 1)
            arr.append(self.read_object())
        return arr

    def _read_dict(self) -> dict:
        d = {}
        while True:
            peek = self.stream.read(1)
            if peek == b'Z':
                break
            self.stream.seek(-1, 1)
            k = self.read_object()
            v = self.read_object()
            d[k] = v
        return d

    def _read_set(self) -> set:
        length = struct.unpack(">I", self._read(4))[0]
        items = set()
        for _ in range(length):
            items.add(self.read_object())
        return items

    def _read_frozenset(self) -> frozenset:
        length = struct.unpack(">I", self._read(4))[0]
        items = []
        for _ in range(length):
            items.append(self.read_object())
        return frozenset(items)

    def _read_tuple(self) -> tuple:
        length = struct.unpack(">I", self._read(4))[0]
        items = []
        for _ in range(length):
            items.append(self.read_object())
        return tuple(items)

    def _read_decimal(self) -> Decimal:
        string_val = self.read_object()  # Read the string representation
        return Decimal(string_val)

    def _read_uuid(self) -> uuid.UUID:
        uuid_bytes = self.read_object()  # Read the bytes
        return uuid.UUID(bytes=uuid_bytes)

    def _read_date(self) -> date:
        year, month, day = struct.unpack(">HBB", self._read(4))
        return date(year, month, day)

    def _read_time(self) -> time:
        hour, minute, second, microsecond = struct.unpack(">BBBI", self._read(7))
        return time(hour, minute, second, microsecond)

    def _read_complex(self) -> complex:
        real, imag = struct.unpack(">dd", self._read(16))
        return complex(real, imag)

    def _read_range(self) -> range:
        start, stop, step = struct.unpack(">iii", self._read(12))
        return range(start, stop, step)

    def _read_bytearray(self) -> bytearray:
        bytes_data = self.read_object()  # Read the bytes
        return bytearray(bytes_data)

    def _read_custom_object(self) -> Any:
        """Deserialize custom objects."""
        class_name = self.read_object()  # Read class name
        obj_data = self.read_object()    # Read object data
        
        # Try to reconstruct the object
        if class_name in self.type_registry:
            cls = self.type_registry[class_name]
            if isinstance(obj_data, dict):
                # Try to create object and set attributes
                try:
                    obj = cls.__new__(cls)
                    obj.__dict__.update(obj_data)
                    return obj
                except Exception:
                    # Fallback: return dict with class info
                    return {"__class__": class_name, "__data__": obj_data}
            else:
                # Data is not a dict, return as-is with class info
                return {"__class__": class_name, "__data__": obj_data}
        else:
            # Unknown class, return dict representation
            return {"__class__": class_name, "__data__": obj_data}
