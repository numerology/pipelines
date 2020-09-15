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
"""Util classes for shareable component spec serialization contract."""
from typing import Any, Dict, List, Optional, Union
from kfp.v2.metadata import artifact_types
from kfp.v2.metadata import parameters


class Yamlable(object):
  """Base class for yaml serialization/deserialization."""
  
  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    """If the child class contains Yamlable object as attr, this needs override."""
    result = cls.__new__(cls)
    result.__dict__ = dict_data
    return result
  
  def to_dict(self) -> Dict[str, Any]:
    # The child class needs to be POD type where all attr can be specified at init.
    return self.__dict__
    

# TypeSpec = [artifact_types.Artifact, parameters.Parameter]

class TypeSpec(Yamlable):
  """This is actually a type alias. Should not have __init__."""
  
  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    if 'Parameter' in dict_data:
      result = cls.__new__(parameters.Parameter)
      result.__dict__ = dict_data['Parameter']
    elif 'Artifact' in dict_data:
      result = cls.__new__(artifact_types.Artifact)
      result.__dict__ = dict_data['Artifact']
    else:
      raise TypeError('Unknown type encountered {}'.format(dict_data))
    
    return result

class InputSpec(Yamlable):
  def __init__(
      self,
      name: str,
      type: TypeSpec,
      description: Optional[str] = None,
  ):
    self.name = name
    self.type = type
    self.description = description
    
  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    result = cls.__new__(cls)
    type_spec = dict_data.pop('type')
    result.__dict__ = dict_data
    result.__dict__['type'] = TypeSpec.from_dict(type_spec)  # dehydrate.
    return result


class OutputSpec(Yamlable):
  def __init__(
      self,
      name: str,
      type: TypeSpec,
      description: Optional[str]
  ):
    self.name = name
    self.type = type
    self.description = description

  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    result = cls.__new__(cls)
    type_spec = dict_data.pop('type')
    result.__dict__ = dict_data
    result.__dict__['type'] = TypeSpec.from_dict(type_spec)  # dehydrate.
    return result
    

class ContainerSpec(Yamlable):
  """For now let's say they are all plain strings."""
  # TODO(numerology): implement placeholder and condition, etc.
  def __init__(
      self,
      image: str,
      args: List[str],
  ):
    self.image = image
    self.args = args


class ContainerImplementationType(Yamlable):
  def __init__(self, container: ContainerSpec):
    self.container = container
  
  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    result = cls.__new__(cls)
    result.__dict__['container'] = ContainerSpec.from_dict(
        dict_data['container'])
    return result


# Later on we might support other types, like service calls
ComponentImplementationType = Union[ContainerImplementationType]


class ComponentSpec(Yamlable):
  """It's safe to assume ComponentSpec has the following fields:
  - name
  - description
  - inputs
  - outputs
  - description
  - implementations:
    - container:
      - image
      - command
      - args
  where some of the fields can contain structural placeholder or objects,
  including: inputs/outputs/args
  
  Thus we can have a strongly typed Python object here.
  """
  def __init__(
      self,
      name: Optional[str] = None,
      description: Optional[str] = None,
      type_def = None,
      inputs: Optional[List[InputSpec]] = None,
      outputs: Optional[List[OutputSpec]] = None,
      implementation: Optional[ComponentImplementationType] = None,
      version: str = 'google.com/cloud/pipelines/component/v1',
  ):
    if type_def:
      raise NotImplementedError('Inlined type definition not supported yet.')
    
    self.name = name,
    self.description = description or 'KFP v2 component'
    self.inputs = inputs
    self.outputs = outputs
    self.implementation = implementation
    self.version = version
    
  @classmethod
  def from_dict(cls, dict_data: Dict[str, Any]) -> Any:
    # inputs/outputs/implementation needs control
    result = cls.__new__(cls)
    inputs_data = dict_data.pop('inputs')
    outputs_data = dict_data.pop('outputs')
    implementation_data = dict_data.pop('implementation')
    
    result.__dict__ = dict_data
    result.__dict__['implementation'] = ContainerImplementationType.from_dict(
        implementation_data)
    inputs = [InputSpec.from_dict(input) for input in inputs_data]
    outputs = [OutputSpec.from_dict(output) for output in outputs_data]
    
    result.__dict__['inputs'] = inputs
    result.__dict__['outputs'] = outputs
    
    return result
    
    

