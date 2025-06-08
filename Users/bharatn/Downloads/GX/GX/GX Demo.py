# Databricks notebook source
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

context_root_dir = "/dbfs/great_expectations/"

# COMMAND ----------

import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.util import get_context
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

context_root_dir = "/dbfs/great_expectations/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=context_root_dir
    ),
)
context = get_context(project_config=data_context_config)

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

# COMMAND ----------

dataframe_datasource = context.sources.add_or_update_spark(
    name="my_spark_in_memory_datasource",
)

dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="yellow_tripdata",
    dataframe=df,
)

# COMMAND ----------

# In a cell:
dbutils.library.restartPython()


# COMMAND ----------

import great_expectations as gx
print(gx.__version__)


# COMMAND ----------

batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

expectation_suite_name = "demo_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "my_databricks_checkpoint"

checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)

# COMMAND ----------

context.add_or_update_checkpoint(checkpoint=checkpoint)

# COMMAND ----------

checkpoint_result = checkpoint.run()

# COMMAND ----------

print(checkpoint.get_config().to_yaml_str())

# COMMAND ----------

html = '/dbfs/great_expectations/uncommitted/data_docs/local_site/index.html'
with open(html, "r") as f:
    data = "".join([l for l in f])
displayHTML(data)

# COMMAND ----------

displayHTML("<a href='/files/great_expectations/uncommitted/data_docs/local_site/index.html' target='_blank'>Open Data Docs</a>")


# COMMAND ----------

dbutils.fs.ls("dbfs:/great_expectations/uncommitted/data_docs/local_site/")

# COMMAND ----------

context = gx.get_context(
    context_root_dir="/dbfs/great_expectations/"
)