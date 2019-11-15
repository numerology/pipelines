#!/usr/bin/env python3
# Copyright 2019 Google LLC
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
"""This sample demonstrates how to use pipeline parameter in TFX DSL.

In this sample, we'll walk through the process of authoring a pipeline with only
one ExampleGen component in TFX DSL. This pipeline consumes an external csv file
whose uri is runtime-parameterized and output an artifact with one split. The
split name is also runtime-parameterized.
"""

import os

from typing import Optional, Text

import kfp
from kfp import dsl

from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen
from tfx.orchestration import pipeline
from tfx.orchestration.experimental.runtime_parameter import runtime_string_parameter
from tfx.orchestration import data_types
from tfx.orchestration.kubeflow import kubeflow_dag_runner
from tfx.utils.dsl_utils import csv_input
from tfx.proto import example_gen_pb2

# Path of pipeline root, should be a GCS path.
pipeline_root = os.path.join(
    'gs://jxzheng-helloworld-kubeflow2-bucket', 'tfx_taxi_simple', kfp.dsl.RUN_ID_PLACEHOLDER
)

# Path to the CSV data file, under which their should be a data.csv file.
# Note: this is still digested as raw PipelineParam b/c parameterization of
# ExternalArtifact attributes has not been implemented yet in TFX.
_data_root_param = dsl.PipelineParam(
    name='data-root',
    value='gs://ml-pipeline-playground/tfx_taxi_simple/data')

# Name of the output split from ExampleGen. Specified as a RuntimeParameter.
_example_split_name = data_types.RuntimeParameter(
    name='split-name', default='train', ptype=Text
)

_example_buckets = data_types.RuntimeParameter(
    name='buckets', default=10, ptype=int
)


def _create_one_step_pipeline(
    pipeline_name: Text,
    pipeline_root: Text,
    enable_cache: Optional[bool] = True
) -> pipeline.Pipeline:
  """Creates a simple TFX pipeline including only an ExampleGen.

  Args:
    pipeline_name: The name of the pipeline.
    pipeline_root: The root of the pipeline output.
    csv_input_location: The location of the input data directory.

  Returns:
    A logical TFX pipeline.Pipeline object.
  """

  examples = csv_input(str(_data_root_param))
  example_gen = CsvExampleGen(
      input=examples,
      output_config={
          'splitConfig': {
              'splits': [
                {'name': _example_split_name, 'hashBuckets': _example_buckets}
              ]
          }
      }
  )
  return pipeline.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      components=[example_gen],
      enable_cache=enable_cache,
  )


if __name__ == '__main__':

  enable_cache = True
  pipeline = _create_one_step_pipeline(
      'dsl_parameter', pipeline_root, enable_cache=enable_cache
  )
  config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
      kubeflow_metadata_config=kubeflow_dag_runner.
      get_default_kubeflow_metadata_config(),
      tfx_image='tensorflow/tfx:latest'
  )
  kfp_runner = kubeflow_dag_runner.KubeflowDagRunner(config=config, output_filename='dsl_parameter.yaml')
  # Make sure kfp_runner recognizes those parameters.
  kfp_runner._params.extend([_data_root_param])

  kfp_runner.run(pipeline)
