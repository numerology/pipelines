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

from kfp.components import _yaml_utils as yaml_utils, _structures as structures
from kfp.v2.metadata import artifact_types
from collections import OrderedDict


# NOT USED!
def node_load_hook(*args):
  print('current args = ')
  print(args)
  # When we enter the artifact descriptor
  # args is a tuple of lists of tuples
  if len(args) == 1 and isinstance(args[0][0], tuple) and 'Artifact' == args[0][0][0]:
    # args[0] list
    # args[0][0] tuple
    print('Constructing artifact')
    #return artifact_types.deserialize_artifacts(**dict())
    
    # args[0][0][0] == 'Artifact'
    # args[0][0][1] is an *already constructed* OrderedDict of data
    return artifact_types.deserialize_artifacts(args[0][0][1])
    # return OrderedDict(args[0][0][1])
  else:
    return OrderedDict(*args)

def _load_component_spec_from_component_text(text) -> structures.ComponentSpec:
  component_dict = yaml_utils.load_yaml(
      stream=text,
      # When use ModelBase do the following line
      # object_pairs_hook=OrderedDict
      object_pairs_hook=dict,
  )
  print('component_dict:\n{}'.format(component_dict))
  
  component_spec = structures.ComponentSpec.from_dict(component_dict)
  
  # Calculating hash digest for the component
  import hashlib
  data = text if isinstance(text, bytes) else text.encode('utf-8')
  data = data.replace(b'\r\n', b'\n')  # Normalizing line endings
  digest = hashlib.sha256(data).hexdigest()
  component_spec._digest = digest
  
  return component_spec

def load_component_from_text(text):
  """Loads component from text and creates a task factory function
  
  Args:
      text: A string containing the component file data.

  Returns:
      A factory function with a strongly-typed signature.
      Once called with the required arguments, the factory constructs a pipeline task instance (ContainerOp).
  """
  if text is None:
    raise TypeError
  component_spec = _load_component_spec_from_component_text(text)
  return component_spec