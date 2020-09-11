# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dummy module re-hydrate serialized artifact and types.

What do we need:
1. Artifact Type ontology
2. Serialization/Deserialization contract. This will be used in two different
   places: 1) load component spec, and 2) rehydrate from container cmd line
   interface
   
   for 1), the object is dehydrated from a dict, (or list, if applicable),
   for 2), the object is loaded from a string

"""

from kfp.components import modelbase


class Artifact(modelbase.ModelBase):
  
  def __init__(self, schema_title):
    # for k, v in kwargs.items():
    #   self.__setattr__(k, v)
    # print(type(kwargs))
    super().__init__(locals())