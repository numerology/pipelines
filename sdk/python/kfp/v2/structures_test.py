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
"""Unit tests for kfp.v2.structures"""

from absl.testing import absltest
from kfp.v2 import structures


_CONTAINER_DICT = {
    'image':'gcr.io/test-image',
    'args': ['python3', 'myentrypoint.py']}
_IMPL_DICT = {'container': _CONTAINER_DICT}
_IN_TYPE_DICT = {'Artifact': {'schema_title': 'aiplatform.Dataset.v1'}}
_OUT_TYPE_DICT = {'Artifact': {'schema_title': 'aiplatform.Model.v1'}}
_INPUTS = [
    {'name': 'dataset_in',
     'description': 'demo input',
     'type': _IN_TYPE_DICT}]
_OUTPUTS = [
    {'name': 'model_out',
     'description': 'demo output',
     'type': _OUT_TYPE_DICT}]
_COMPONENT_DICT = {
    'name': 'My Demo Component',
    'description': 'test',
    'inputs': _INPUTS,
    'outputs': _OUTPUTS,
    'implementation': _IMPL_DICT
}


class StructuresTest(absltest.TestCase):
  def testFromDict(self):
    component_spec = structures.ComponentSpec.from_dict(_COMPONENT_DICT)
    self.assertIsInstance(component_spec, structures.ComponentSpec)
    self.assertEqual(component_spec.name, 'My Demo Component')
    self.assertEqual(
        component_spec.implementation.container.image,
        'gcr.io/test-image')
