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

import os

from typing import Text

import kfp
from kubernetes import client as k8s_client
from kfp import dsl
from kfp import gcp
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
from tfx.proto import evaluator_pb2
from tfx.utils.dsl_utils import external_input
from tfx.proto import pusher_pb2
from tfx.proto import trainer_pb2
from tfx.orchestration.kubeflow.proto import kubeflow_pb2

# Define pipeline params used for pipeline execution.
# Path to the module file, should be a GCS path.
_taxi_module_file_param = dsl.PipelineParam(
    name='module-file',
    value='gs://ml-pipeline-playground/tfx_taxi_simple/modules/taxi_utils.py'
)

# Path to the CSV data file, under which their should be a data.csv file.
_data_root_param = dsl.PipelineParam(
    name='data-root', value='gs://ml-pipeline-playground/tfx_taxi_simple/data'
)

# Path of pipeline root, should be a GCS path.
pipeline_root = os.path.join(
    'gs://your-bucket', 'tfx_taxi_simple', kfp.dsl.RUN_ID_PLACEHOLDER
)

_KUBEFLOW_GCP_SECRET_NAME = 'user-gcp-sa'

def _create_test_pipeline(
    pipeline_root: Text, csv_input_location: Text, taxi_module_file: Text,
    enable_cache: bool
):
  """Creates a simple Kubeflow-based Chicago Taxi TFX pipeline.

  Args:
    pipeline_root: The root of the pipeline output.
    csv_input_location: The location of the input data directory.
    taxi_module_file: The location of the module file for Transform/Trainer.
    enable_cache: Whether to enable cache or not.

  Returns:
    A logical TFX pipeline.Pipeline object.
  """
  examples = external_input(csv_input_location)

  example_gen = CsvExampleGen(input=examples)
  statistics_gen = StatisticsGen(input_data=example_gen.outputs['examples'])
  infer_schema = SchemaGen(
      stats=statistics_gen.outputs['statistics'],
      infer_feature_shape=False,
  )
  validate_stats = ExampleValidator(
      stats=statistics_gen.outputs['statistics'],
      schema=infer_schema.outputs['schema'],
  )
  transform = Transform(
      input_data=example_gen.outputs['examples'],
      schema=infer_schema.outputs['schema'],
      module_file=taxi_module_file,
  )
  trainer = Trainer(
      module_file=taxi_module_file,
      transformed_examples=transform.outputs['transformed_examples'],
      schema=infer_schema.outputs['schema'],
      transform_output=transform.outputs['transform_graph'],
      train_args=trainer_pb2.TrainArgs(num_steps=10),
      eval_args=trainer_pb2.EvalArgs(num_steps=5),
  )
  model_analyzer = Evaluator(
      examples=example_gen.outputs['examples'],
      model_exports=trainer.outputs['model'],
      feature_slicing_spec=evaluator_pb2.FeatureSlicingSpec(
          specs=[
              evaluator_pb2.SingleSlicingSpec(
                  column_for_slicing=['trip_start_hour']
              )
          ]
      ),
  )
  model_validator = ModelValidator(
      examples=example_gen.outputs['examples'], model=trainer.outputs['model']
  )

  # Hack: ensuring push_destination can be correctly parameterized and interpreted.
  # pipeline root will be specified as a dsl.PipelineParam with the name
  # pipeline-root, see:
  # https://github.com/tensorflow/tfx/blob/1c670e92143c7856f67a866f721b8a9368ede385/tfx/orchestration/kubeflow/kubeflow_dag_runner.py#L226
  _pipeline_root_param = dsl.PipelineParam(name='pipeline-root')
  pusher = Pusher(
      model_export=trainer.outputs['model'],
      model_blessing=model_validator.outputs['blessing'],
      push_destination=pusher_pb2.PushDestination(
          filesystem=pusher_pb2.PushDestination.Filesystem(
              base_directory=os.path.
              join(str(_pipeline_root_param), 'model_serving')
          )
      ),
  )

  return pipeline.Pipeline(
      pipeline_name='parameterized_tfx_oss_1',
      pipeline_root=pipeline_root,
      components=[
          example_gen, statistics_gen, infer_schema, validate_stats, transform,
          trainer, model_analyzer, model_validator, pusher
      ],
      enable_cache=enable_cache,
  )

def _mount_secret_op(secret_name: Text):
  """Mounts all key-value pairs found in the named Kubernetes Secret.

  All key-value pairs in the Secret are mounted as environment variables.

  Args:
    secret_name: The name of the Secret resource.

  Returns:
    An OpFunc for mounting the Secret.
  """

  def mount_secret(container_op: dsl.ContainerOp):
    secret_ref = k8s_client.V1ConfigMapEnvSource(
        name=secret_name, optional=True)

    container_op.container.add_env_from(
        k8s_client.V1EnvFromSource(secret_ref=secret_ref))

  return mount_secret

def _mount_config_map_op(config_map_name: Text):
  """Mounts all key-value pairs found in the named Kubernetes ConfigMap.

  All key-value pairs in the ConfigMap are mounted as environment variables.

  Args:
    config_map_name: The name of the ConfigMap resource.

  Returns:
    An OpFunc for mounting the ConfigMap.
  """

  def mount_config_map(container_op: dsl.ContainerOp):
    config_map_ref = k8s_client.V1ConfigMapEnvSource(
        name=config_map_name, optional=True)
    container_op.container.add_env_from(
        k8s_client.V1EnvFromSource(config_map_ref=config_map_ref))

  return mount_config_map

def get_default_pipeline_operator_funcs():
  """Returns a default list of pipeline operator functions.

  Returns:
    A list of functions with type OpFunc.
  """
  # Enables authentication for GCP services in a typical GKE Kubeflow
  # installation.
  gcp_secret_op = gcp.use_gcp_secret(_KUBEFLOW_GCP_SECRET_NAME)

  # Mounts configmap containing the MySQL DB to use for logging metadata.
  mount_config_map_op = _mount_config_map_op('metadata-db-parameters')

  # Mounts the secret containing the MySQL DB password.
  mysql_password_op = _mount_secret_op('metadata-db-secrets')

  return [gcp_secret_op, mount_config_map_op, mysql_password_op]

def get_default_kubeflow_metadata_config(
) -> kubeflow_pb2.KubeflowMetadataConfig:
  """Returns the default metadata connection config for Kubeflow.

  Returns:
    A config proto that will be serialized as JSON and passed to the running
    container so the TFX component driver is able to communicate with MLMD in
    a Kubeflow cluster.
  """
  config = kubeflow_pb2.KubeflowMetadataConfig()
  config.mysql_db_service_host.value = 'metadata-db'
  config.mysql_db_service_port.value = '3306'
  config.mysql_db_name.value = 'metadb'
  config.mysql_db_user.value = 'root'
  config.mysql_db_password.environment_variable = 'MYSQL_ROOT_PASSWORD'

  return config

if __name__ == '__main__':

  enable_cache = True
  pipeline = _create_test_pipeline(
      pipeline_root,
      str(_data_root_param),
      str(_taxi_module_file_param),
      enable_cache=enable_cache,
  )
  # Make sure the version of TFX image used is consistent with the version of
  # TFX SDK. Here we use tfx:0.15.0 image.
  config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
      kubeflow_metadata_config=get_default_kubeflow_metadata_config(),
      pipeline_operator_funcs=get_default_pipeline_operator_funcs(),
      tfx_image='tensorflow/tfx:0.15.0',
  )
  kfp_runner = kubeflow_dag_runner.KubeflowDagRunner(
      output_filename=__file__ + '.yaml', config=config
  )
  # Make sure kfp_runner recognizes those parameters.
  kfp_runner._params.extend([_data_root_param, _taxi_module_file_param])

  kfp_runner.run(pipeline)
