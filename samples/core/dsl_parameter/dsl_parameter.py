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

In this sample, we'll walk through the process of authoring a pipeline with
RuntimeParameter. In principle RuntimeParameter can be used as common Python
objects. However if the object to be parameterized happens to be a field in a
protobuf message, then we have to construct a dictionary with exactly the same
structure (key words, nested structures etc.) and put the RuntimeParameter with
the same key as its field name in the protobuf spec.
"""

import os

from typing import Optional, Text

import kfp
from kfp import dsl

from tfx.orchestration import data_types
from tfx.components.evaluator.component import Evaluator
from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen
from tfx.components.example_validator.component import ExampleValidator
from tfx.components.model_validator.component import ModelValidator
from tfx.components.pusher.component import Pusher
from tfx.components.schema_gen.component import SchemaGen
from tfx.components.statistics_gen.component import StatisticsGen
from tfx.components.trainer.component import Trainer
from tfx.components.transform.component import Transform
from tfx.orchestration import pipeline
from tfx.orchestration.kubeflow import kubeflow_dag_runner
from tfx.utils.dsl_utils import external_input
from tfx.proto import pusher_pb2

# Path of pipeline root, should be a GCS path.
pipeline_root = os.path.join(
    'gs://jxzheng-helloworld-kubeflow2-bucket', 'tfx_taxi_simple', kfp.dsl.RUN_ID_PLACEHOLDER
)

# Path to the CSV data file, under which their should be a data.csv file.
# Note: this is still digested as raw PipelineParam b/c parameterization of
# ExternalArtifact attributes has not been implemented yet in TFX.
_data_root_param = data_types.RuntimeParameter(
    name='data-root',
    default='gs://ml-pipeline-playground/tfx_taxi_simple/data',
    ptype=Text,
)

# Path to the module file
_taxi_module_file_param = data_types.RuntimeParameter(
    name='module-file',
    default='gs://ml-pipeline-playground/tfx_taxi_simple/modules/taxi_utils.py',
    ptype=Text,
)

_train_steps = data_types.RuntimeParameter(
    name='train-steps',
    default=10,
    ptype=int,
)

_eval_args = data_types.RuntimeParameter(
    name='eval-steps',
    default=5,
    ptype=int,
)

_slicing_column = data_types.RuntimeParameter(
    name='slicing-column',
    default='trip_start_hour',
    ptype=Text,
)

def _create_parameterized_pipeline(
    pipeline_name: Text,
    pipeline_root: Text,
    enable_cache: Optional[bool] = True
) -> pipeline.Pipeline:
  """Creates a simple TFX pipeline with RuntimeParameter.

  Args:
    pipeline_name: The name of the pipeline.
    pipeline_root: The root of the pipeline output.
    csv_input_location: The location of the input data directory.

  Returns:
    A logical TFX pipeline.Pipeline object.
  """
  examples = external_input(_data_root_param)
  example_gen = CsvExampleGen(
      input=examples
  )

  statistics_gen = StatisticsGen(input_data=example_gen.outputs.examples)
  infer_schema = SchemaGen(
      stats=statistics_gen.outputs.output, infer_feature_shape=False)
  validate_stats = ExampleValidator(
      stats=statistics_gen.outputs.output, schema=infer_schema.outputs.output)
  transform = Transform(
      input_data=example_gen.outputs.examples,
      schema=infer_schema.outputs.output,
      module_file=_taxi_module_file_param)
  trainer = Trainer(
      module_file=_taxi_module_file_param,
      transformed_examples=transform.outputs.transformed_examples,
      schema=infer_schema.outputs.output,
      transform_output=transform.outputs.transform_output,
      train_args=dict(num_steps=_train_steps),
      eval_args=dict(num_steps=_eval_args))
  model_analyzer = Evaluator(
      examples=example_gen.outputs.examples,
      model_exports=trainer.outputs.output,
      feature_slicing_spec=dict(
          specs=[
              dict(column_for_slicing=[_slicing_column])
          ]
      ))
  model_validator = ModelValidator(
      examples=example_gen.outputs.examples, model=trainer.outputs.output)

  # Hack: ensuring push_destination can be correctly parameterized and interpreted.
  # pipeline root will be specified as a dsl.PipelineParam with the name
  # pipeline-root, see:
  # https://github.com/tensorflow/tfx/blob/1c670e92143c7856f67a866f721b8a9368ede385/tfx/orchestration/kubeflow/kubeflow_dag_runner.py#L226
  _pipeline_root_param = dsl.PipelineParam(name='pipeline-root')
  pusher = Pusher(
      model_export=trainer.outputs.output,
      model_blessing=model_validator.outputs.blessing,
      push_destination=pusher_pb2.PushDestination(
          filesystem=pusher_pb2.PushDestination.Filesystem(
              base_directory=os.path.join(
                  str(_pipeline_root_param), 'model_serving'))))


  return pipeline.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      components=[
        example_gen, statistics_gen, infer_schema, validate_stats, transform,
        trainer, model_analyzer, model_validator, pusher
      ],
      enable_cache=enable_cache,
  )


if __name__ == '__main__':

  enable_cache = True
  pipeline = _create_parameterized_pipeline(
      'new_parameter_new_module', pipeline_root, enable_cache=enable_cache
  )
  config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
      kubeflow_metadata_config=kubeflow_dag_runner.
      get_default_kubeflow_metadata_config(),
      tfx_image='tensorflow/tfx:latest'
  )
  kfp_runner = kubeflow_dag_runner.KubeflowDagRunner(config=config, output_filename='dsl_parameter.yaml')

  kfp_runner.run(pipeline)
