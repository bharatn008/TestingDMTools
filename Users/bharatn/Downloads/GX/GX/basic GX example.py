# Databricks notebook source
import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.util import get_context
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

context_root_dir = "/dbfs/great_expectations_simple/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=context_root_dir
    ),
)
context = get_context(project_config=data_context_config)

# COMMAND ----------


# Define datasource for files
datasource = context.sources.add_pandas(name="people_data_sources")
data_asset = datasource.add_csv_asset(
    name="people_data_asset",
    filepath_or_buffer="/dbfs/FileStore/tables/people_data.csv"
)
batch = data_asset.build_batch_request()

# COMMAND ----------

suite = context.add_or_update_expectation_suite("people_suite")

# COMMAND ----------

dbutils.fs.ls("dbfs:/great_expectations-bharat")

# COMMAND ----------

# Validate: add expectations
validator = context.get_validator(
    batch_request=batch,
    expectation_suite_name="people_suite"
)

# COMMAND ----------

validator.head(10)

# COMMAND ----------

validator.expect_column_to_exist("name")
validator.expect_column_values_to_be_between("age", min_value=10, max_value=30)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

from great_expectations.checkpoint import SimpleCheckpoint

# COMMAND ----------

# Create checkpoint
checkpoint = SimpleCheckpoint(
    name="people_checkpoint",
    data_context=context,
    batch_request=batch,
    expectation_suite_name="people_suite"
)
context.add_or_update_checkpoint(checkpoint=checkpoint)

# COMMAND ----------

checkpoint_result = checkpoint.run()
print(checkpoint_result)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Read the file content as text before
file_path = "dbfs:/great_expectations/checkpoints/my_databricks_checkpoint.yml"

with open("/dbfs/great_expectations/checkpoints/my_databricks_checkpoint.yml", "r") as f:
    content = f.read()

print(content)


# COMMAND ----------

# Read the file content as text before
file_path = "/dbfs/great_expectations_simple/expectations/people_suite.json"

with open(file_path, "r") as f:
    content = f.read()

print(content)
