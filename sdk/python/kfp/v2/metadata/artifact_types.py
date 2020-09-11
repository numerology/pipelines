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

For CUJ 2 the first one is what we need.
"""
import importlib
from typing import Any, Dict, Text, Type

from kfp.components import modelbase

# Consider using frozendict
SCHEMA_TO_TYPE_PATH = {
    'aiplatform.Dataset.v1': 'kfp.v2.metadata.artifact_types.Dataset',
    'aiplatform.Model.v1': 'kfp.v2.metadata.artifact_types.Model',
}


def import_class_by_path(class_path: Text) -> Type[Any]:
  """Import a class by its <module>.<name> path.

  Args:
    class_path: <module>.<name> for a class.

  Returns:
    Class object for the given class_path.
  """
  classname = class_path.split('.')[-1]
  modulename = '.'.join(class_path.split('.')[0:-1])
  mod = importlib.import_module(modulename)
  return getattr(mod, classname)

# BEGIN: ontology

class Artifact(modelbase.ModelBase):
  
  def __init__(self, schema_title):
    # for k, v in kwargs.items():
    #   self.__setattr__(k, v)
    # print(type(kwargs))
    super().__init__(locals())


class Dataset(Artifact):
  def __init__(self, schema_title):
    super().__init__(locals())


class Model(Artifact):
  def __init__(self, schema_title):
    super().__init__(locals())

# BEGIN: helpers

def deserialize_artifacts(dict_data: Dict[Text, Any]) -> Artifact:
  """Deserialize artifact from dict."""
  if not dict_data.get('schema_title', None):
    raise RuntimeError('Where is the title?')
  
  class_path = SCHEMA_TO_TYPE_PATH.get(dict_data.get('schema_title'))
  artifact_class = import_class_by_path(class_path)
  return artifact_class(schema_title=dict_data.get('schema_title'))
  