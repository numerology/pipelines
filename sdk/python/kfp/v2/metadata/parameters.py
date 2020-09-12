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

"""Dummy module re-hydrate serialized Parameters.

"""

import sys
import importlib
from typing import Any, Dict, Mapping, Text, Type

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

class Parameter(modelbase.ModelBase):
  
  def __init__(self, type):
    super().__init__(locals())
  
  @classmethod
  def from_dict(cls, struct: Mapping) -> 'Parameter':
    if not struct.get('type', None):
      raise RuntimeError('Where is the type?')
  
    class_path = 'kfp.v2.metadata.parameters.Parameter'  # self pointer
    self_class = import_class_by_path(class_path)
    return self_class(type=struct.get('type'))