# Databricks notebook source
# DBTITLE 1,Widget Parameters
dbutils.widgets.text("context_root_dir", "/dbfs/mnt/gxtesting/bharat", "GE Context Root Directory")
dbutils.widgets.text("suite_name", "taxi_suite", "Expectation Suite Name")
dbutils.widgets.text("data_asset_name", "taxi_data", "Data Asset Name")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-05-07T14:00:08Z&st=2025-04-30T06:00:08Z&spr=https&sig=j5xdYA9cjH9GbzVw21kgTzHu2%2BVkNfmbQrejGub1Efg%3D"  # <- your SAS token

# COMMAND ----------



# COMMAND ----------

import pandas as pd

# COMMAND ----------

# 1. Define variables
storage_account_name = "azurepracticeblbstg"
container_name = "gxtesting"

# 2. Build source URL
source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

# 3. Mount it
dbutils.fs.mount(
  source = source_url,
  mount_point = f"/mnt/{container_name}",
  extra_configs = {
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token
  }
)

print("✅ Mount successful!")


# COMMAND ----------

dbutils.fs.unmount("/mnt/gxtesting")

# COMMAND ----------

import great_expectations as gx

# 1. Initialize GX manually
context = gx.data_context.FileDataContext.create(
    project_root_dir="/dbfs/mnt/gxtesting/great_expectations"
)

print("✅ GX context created at /mnt/gxtesting/great_expectations")


# COMMAND ----------

context.list_expectation_suite_names()

# COMMAND ----------


# 3. Get the list of available expectation suite names
print("Available expectation suites:", context.list_expectation_suite_names())

# Replace 'your_expectation_suite_name' with the actual name from the list above
suite_name = "blob_people_suiteame"

dataset = gx.dataset.PandasDataset(
    df,
    expectation_suite=suite_name
)

# 6. Validate the data
validation_result = dataset.validate()

# 6. Print validation results summary 
print(f"Successful: {validation_result['success']}")
print(f"Results: {validation_result['statistics']}")

# 7. Optionally build and view data docs
context.build_data_docs()

# COMMAND ----------


# Define datasource for files
datasource = context.sources.add_pandas(name="sample_taxi")
data_asset = datasource.add_csv_asset(
    name="people_data_asset",
    filepath_or_buffer="/dbfs/FileStore/tables/people_data.csv"
)
batch = data_asset.build_batch_request()
suite = context.add_or_update_expectation_suite("blob_people_suite")

# Validate: add expectations
validator = context.get_validator(
    batch_request=batch,
    expectation_suite_name="blob_people_suite"
)



# COMMAND ----------

# MAGIC %md
# MAGIC Getting context
# MAGIC

# COMMAND ----------

suite_name = "blob_people_suite"  # Replace with your actual suite name
suite = context.get_expectation_suite(suite_name)

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

# COMMAND ----------

batch_request = {
    "datasource_name": "taxi-sample-datasource",  # Should match one in great_expectations.yml
    "data_connector_name": "default_runtime_data_connector_name",
    "data_asset_name": "taxi-asset",  # Arbitrary name
    "runtime_parameters": {"batch_data": df},
    "batch_identifiers": {"default_identifier_name": "default_id"},
}


# COMMAND ----------

context.list_datasources

# COMMAND ----------

# suite_name = "your_suite_name"
suite = context.suites.add(suite_name)

# Get Validator from pandas DataFrame using Fluent API
validator = context.sources.pandas_default.read_dataframe(df)

# Attach the suite (optional if you're going to add expectations manually)
# validator.expectation_suite_name = suite_name

# Add expectations (if needed)
# validator.expect_column_values_to_not_be_null("column_name")
# validator.expect_column_values_to_be_in_set("column_name", ["value1", "value2"])

# Run validations (no arguments here)
result = validator.validate()

# Print or save results
print(result)

# COMMAND ----------

# Step 2: Create a sample DataFrame
data = {
    "name": ["Alice", "Bob", "Charlie", None],
    "age": [25, 30, 35, 40]
}
import pandas as pd
df = pd.DataFrame(data)

# Step 3: Get the Expectation Suite from the context
suite = context.get_expectation_suite(suite_name)

# Step 4: Create a Validator for the DataFrame
# Assuming you have a datasource already configured
datasource_name = "sdsdsds"  # Replace with your actual datasource name



# Create a Validator for the batch
validator = context.get_validator(
    datasource_name=datasource_name,
    data_asset_name="people_data_asset",  # Replace with your actual data asset name
    expectation_suite_name=suite_name,
    batch=batch_request
)

# Step 5: Run the expectations (validations) on the DataFrame
results = validator.run()

# COMMAND ----------



import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

new_data = pd.DataFrame({
    "name": ["George", "Hannah", "Isaac"],
    "age": [22, 31, 25],
    "salary": [48000, 52000, 59000]
})

spark_df = spark.createDataFrame(new_data)


suite_name = "blob_people_suite"  # Your suite name
suite = context.get_expectation_suite(suite_name)

batch_kwargs = {
    "datasource": "spark_datasource",  # Name of the Spark datasource in your GX config
    "data_asset_name": "my_spark_asset",  # The name for this particular data asset
    "batch_data": spark_df,  # The actual Spark DataFrame
}

validator = context.get_validator(
    datasource_name="newone",  # Your actual Spark datasource name
    data_asset_name="onewone",  # The data asset name
    expectation_suite_name=suite_name,  # The suite name
    batch_kwargs=batch_kwargs  # The batch (data) to validate
)

# Step 8: Run the expectations on the data
results = validator.validate()

# Step 9: Print validation results
print("Validation Results:")
print(results)

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="people_blob_checkpoint", validator=validator
)

# COMMAND ----------

checkpoint_result = checkpoint.run()

# COMMAND ----------

# MAGIC %md
# MAGIC end

# COMMAND ----------



# COMMAND ----------

validator.save_expectation_suite() 

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="people_blob_checkpoint", validator=validator
)

# COMMAND ----------

checkpoint_result = checkpoint.run()

# COMMAND ----------

validator.expect_column_to_exist("name")
validator.expect_column_values_to_be_between("age", min_value=10, max_value=30)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

from great_expectations.checkpoint import SimpleCheckpoint

# Create checkpoint
checkpoint = SimpleCheckpoint(
    name="people_blob_checkpoint",
    data_context=context,
    batch_request=batch,
    expectation_suite_name="blob_people_suite"
)
context.add_or_update_checkpoint(checkpoint=checkpoint)

checkpoint_result = checkpoint.run()
print(checkpoint_result)

# COMMAND ----------

validation_result = validator.validate()

# COMMAND ----------

validation_result

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/gxtesting")

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

context_root_dir = "/dbfs/great_expectations_simple/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=context_root_dir
    ),
)
context = get_context(project_config=data_context_config)

# COMMAND ----------

import great_expectations as ge
import pandas as pd

# Initialize Data Context
context = ge.data_context.DataContext("/path/to/great_expectations/directory")

# Example: List of 13 tables, represented as DataFrames here (in reality, they would come from your data source)
tables = {
    "table1": pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 40],
        "salary": [50000, 55000, 60000]
    }),
    "table2": pd.DataFrame({
        "name": ["David", "Eve", "Frank"],
        "age": [28, 35, 22],
        "salary": [60000, 65000, 70000]
    }),
    # Add more tables similarly...
}

# Define your expectations (these could be rules to be applied across all tables)
expectations = [
    {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {
            "column": "name"
        },
        "meta": {}
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {
            "column": "age",
            "min_value": 10,
            "max_value": 30
        },
        "meta": {}
    }
]

# Define the same expectation suite name (this will be the same for all tables)
suite_name = "my_suite"

# Create a single expectation suite
suite = context.create_expectation_suite(suite_name=suite_name, overwrite_existing=True)

# Loop through each table and dynamically apply the expectations
for table_name, df in tables.items():
    # Loop through each expectation rule
    for expectation in expectations:
        column = expectation["kwargs"]["column"]
        
        # Check if the column exists in the current table
        if column in df.columns:
            # Modify the column name in the expectation kwargs dynamically if needed
            updated_kwargs = expectation["kwargs"].copy()
            updated_kwargs["column"] = column  # Ensure we're applying the right column to the expectation

            # Add the expectation to the suite for this column
            context.add_expectation(
                suite=suite,
                expectation_type=expectation["expectation_type"],
                kwargs=updated_kwargs
            )

    # Now, validate this table against the suite (this part can be done later as well)
    validator = context.get_validator(
        datasource_name="my_datasource",
        data_asset_name=table_name,
        expectation_suite_name=suite.suite_name,
    )

    # Validate the data and run expectations
    validation_result = validator.validate()

    # Print the validation results (pass/fail)
    print(f"Validation Results for {table_name}:")
    print(validation_result)

    # Optionally, you can store or process the validation results here


# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="my_taxi_validator_checkpoint", validator=validator
)