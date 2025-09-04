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

from typing import Any, Callable, Protocol, Optional


class JsonSerializerPlugin(Protocol):
    """Protocol for JSON serialization plugins"""

    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...
    def can_handle(self, obj: Any) -> bool: ...


class TypeHandlerPlugin(Protocol):
    """Protocol for type-specific serialization"""

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool: ...
    def serialize_to_dict(self, obj: Any) -> Any: ...


class SimpleRegistry:
    """Simplified registry using dict instead of complex TypeProviderRegistry"""

    def __init__(self):
        # Simple dict mapping: type -> handler function
        self.type_handlers: dict[type, Callable[..., Any]] = {}
        self.plugins: list[TypeHandlerPlugin] = []

    def register_type_handler(self, obj_type: type, handler: Callable[..., Any]) -> None:
        """Register a simple type handler function"""
        self.type_handlers[obj_type] = handler

    def register_plugin(self, plugin: TypeHandlerPlugin) -> None:
        """Register a plugin"""
        self.plugins.append(plugin)

    def get_handler(self, obj: Any) -> Optional[Callable[..., Any]]:
        """Get handler for object - check dict first, then plugins"""
        obj_type = type(obj)
        if obj_type in self.type_handlers:
            return self.type_handlers[obj_type]

        for plugin in self.plugins:
            if plugin.can_serialize_type(obj, obj_type):
                return plugin.serialize_to_dict
        return None


class SerializationException(Exception):
    """Exception raised during serialization"""

    pass


class DeserializationException(Exception):
    """Exception raised during deserialization"""

    pass
