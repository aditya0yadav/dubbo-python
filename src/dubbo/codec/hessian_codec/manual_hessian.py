import io
import struct
from typing import Any

class Hessian2Output:
    def __init__(self, stream: io.BytesIO):
        self.stream = stream

    def write_object(self, obj: Any):
        if obj is None:
            self.stream.write(b'N')  # null
        elif isinstance(obj, bool):
            self.stream.write(b'T' if obj else b'F')
        elif isinstance(obj, int):
            if -16 <= obj <= 47:
                self.stream.write(bytes([0x90 + obj]))
            elif -2048 <= obj <= 2047:
                self.stream.write(bytes([0xc8 + (obj >> 8), obj & 0xff]))
            else:
                self.stream.write(b'I' + struct.pack(">i", obj))
        elif isinstance(obj, float):
            self.stream.write(b'D' + struct.pack(">d", obj))
        elif isinstance(obj, str):
            encoded = obj.encode("utf-8")
            length = len(encoded)
            if length <= 31:
                self.stream.write(bytes([length]) + encoded)
            else:
                self.stream.write(b'S' + struct.pack(">H", length) + encoded)
        elif isinstance(obj, bytes):
            length = len(obj)
            self.stream.write(b'B' + struct.pack(">H", length) + obj)
        elif isinstance(obj, list):
            self.stream.write(b'V')
            for item in obj:
                self.write_object(item)
            self.stream.write(b'Z')
        elif isinstance(obj, dict):
            self.stream.write(b'M')
            for k, v in obj.items():
                self.write_object(k)
                self.write_object(v)
            self.stream.write(b'Z')
        else:
            raise TypeError(f"Unsupported type for Hessian encoding: {type(obj)}")


class Hessian2Input:
    def __init__(self, stream: io.BytesIO):
        self.stream = stream

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
        tag = self._read_byte()
        
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
            arr = []
            while True:
                peek = self.stream.read(1)
                if peek == b'Z':
                    break
                self.stream.seek(-1, 1)
                arr.append(self.read_object())
            return arr
        
        elif tag == ord('M'):
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
        
        else:
            raise ValueError(f"Unknown Hessian tag: {hex(tag)}")
