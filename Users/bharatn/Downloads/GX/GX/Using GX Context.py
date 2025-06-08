# Databricks notebook source
dbutils.fs.ls("dbfs:/great_expectations/checkpoints/")

# COMMAND ----------

# Read the file content as text before
file_path = "dbfs:/great_expectations/checkpoints/my_databricks_checkpoint.yml"

with open("/dbfs/great_expectations/checkpoints/my_databricks_checkpoint.yml", "r") as f:
    content = f.read()

print(content)


# COMMAND ----------

# Read the file content as text before
file_path = "dbfs:/great_expectations/checkpoints/my_databricks_checkpoint.yml"

with open("/dbfs/great_expectations/checkpoints/my_databricks_checkpoint.yml", "r") as f:
    content = f.read()

print(content)


# COMMAND ----------

# MAGIC %pip install great_expectations==0.18.14
# MAGIC

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

import pandas

# COMMAND ----------

df = pandas.read_csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")


# COMMAND ----------

df

# COMMAND ----------

dataframe_asset = context.sources.add_pandas(
    "dasdf"
).add_dataframe_asset(
    name="taxi_frame",
    dataframe=df,
    batch_metadata={"year": "2019", "month": "01"},
)


batch_request = dataframe_asset.build_batch_request()
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
# validator.save_expectation_suite()

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="my_databricks_checkpoint",
    batch_request=batch_request,
    expectation_suite_name="demo_suite",
)

checkpoint_result = checkpoint.run()

# COMMAND ----------

print(checkpoint_result)

# COMMAND ----------

suite = context.get_expectation_suite("demo_suite")
for exp in suite.expectations:
    print(exp.expectation_type, exp.kwargs)


# COMMAND ----------

# import pandas as pd
# import great_expectations as gx
# from great_expectations.core.batch import BatchRequest

# 1️⃣ Create a dummy DataFrame
df = pandas.DataFrame({
    "passenger_count": [1, 2, 3, 4, 2],
    "fare_amount": [5.5, 12.0, 7.5, 10.0, 3.5],
    "payment_type": ["Cash", "Card", "Card", "Cash", "No Charge"]
})

# 2️⃣ Create or load the context
# context = gx.get_context()

# 3️⃣ Register the dataframe as a source asset
dataframe_asset = context.sources.add_pandas(
    name="demfddooo"
).add_dataframe_asset(
    name="taxi_frame",
    dataframe=df,
    batch_metadata={"source": "databricks_sample"}
)

# 4️⃣ Build the batch request
batch_request = dataframe_asset.build_batch_request()

# 5️⃣ Create or update the expectation suite
suite = context.add_or_update_expectation_suite("demo_suite")

# 6️⃣ Get a validator for the data & suite
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

# 7️⃣ Add some expectations
# Add a non-null check for a column
validator.expect_column_values_to_not_be_null("passenger_count")

# Ensure all fare_amount values are greater than 0
validator.expect_column_values_to_be_greater_than(
    column="fare_amount", 
    value=0
)

# Expect column values to be within a set of predefined payment types
validator.expect_column_values_to_be_in_set(
    column="payment_type", 
    value_set=["Cash", "Card", "No Charge"]
)

# 8️⃣ Save the expectation suite
validator.save_expectation_suite()

# 9️⃣ Run a checkpoint using this suite
checkpoint = context.add_or_update_checkpoint(
    name="my_databricks_checkpoint",
    batch_request=batch_request,
    expectation_suite_name="demo_suite"
)

result = checkpoint.run()

# 🔥 Open Data Docs (if configured) to see a nice HTML report
context.open_data_docs()
