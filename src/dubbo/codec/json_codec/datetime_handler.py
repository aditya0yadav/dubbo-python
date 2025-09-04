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

from datetime import datetime, date, time
from typing import Any, Dict, Union

from dubbo.codec.json_codec import TypeHandler

__all__ = ["DateTimeHandler"]


class DateTimeHandler(TypeHandler):
    """
    Type handler for datetime, date, and time objects.

    Serializes datetime objects to ISO format with timezone information.
    """

    def can_serialize_type(self, obj: Any, obj_type: type) -> bool:
        """
        Check if this handler can serialize datetime-related types.

        :param obj: The object to check.
        :type obj: Any
        :param obj_type: The type of the object.
        :type obj_type: type
        :return: True if object is datetime, date, or time.
        :rtype: bool
        """
        return isinstance(obj, (datetime, date, time))

    def serialize_to_dict(self, obj: Union[datetime, date, time]) -> Dict[str, Any]:
        """
        Serialize datetime objects to dictionary representation.

        :param obj: The datetime object to serialize.
        :type obj: Union[datetime, date, time]
        :return: Dictionary representation with type markers.
        :rtype: Dict[str, Any]
        """
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat(), "__timezone__": str(obj.tzinfo) if obj.tzinfo else None}
        elif isinstance(obj, date):
            return {"__date__": obj.isoformat()}
        elif isinstance(obj, time):
            return {"__time__": obj.isoformat()}
        else:
            raise ValueError(f"Unsupported datetime type: {type(obj)}")
