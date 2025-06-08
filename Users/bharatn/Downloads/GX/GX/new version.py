# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt")

# COMMAND ----------

context_root_dir = "dbfs/mnt/gxtesting/great_expectations"

# COMMAND ----------

context = gx.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

context.add_datasource(
    name="my_filesystem_data_source",
    base_directory=context_root_dir,
    data_asset_type={"module_name": "pandas", "class_name": "PandasDataset"},)

# COMMAND ----------

context.list_expectation_suite_names()