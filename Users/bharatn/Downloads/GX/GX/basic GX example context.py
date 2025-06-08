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
    filepath_or_buffer="/dbfs/FileStore/tables/people_data2.csv"
)
batch = data_asset.build_batch_request()

# COMMAND ----------

context.profile_data_asset(data_asset_name="people_data_asset", datasource_name="people_data_sources")


# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="people_checkpoint",
    batch_request=batch,
    expectation_suite_name="people_suite",
)

checkpoint_result = checkpoint.run()

# COMMAND ----------

print(checkpoint_result)