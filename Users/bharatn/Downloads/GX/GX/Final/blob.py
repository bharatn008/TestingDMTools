# Databricks notebook source

expectation_json = {
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {
        "column": "trip_distance",
        "min_value": 0.1,
        "max_value": 100
    },
    "meta": {
        "created_by": "user@example.com",
        "created_at": "2025-04-30"
    }
}


# COMMAND ----------



# COMMAND ----------

import pandas
df = pandas.read_csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")


# COMMAND ----------

import great_expectations as gx

# 1. Initialize GX manually
context = gx.data_context.FileDataContext.create(
    project_root_dir="/dbfs/mnt/gxtesting/great_expectations"
)

print("GX context created at /mnt/gxtesting/great_expectations")

# COMMAND ----------

import great_expectations as gx
from great_expectations.data_context import FileDataContext

context = gx.data_context.FileDataContext.create(project_root_dir="/dbfs/mnt/gxtesting/great_expectations")


# COMMAND ----------

context.list_checkpoints

# COMMAND ----------

batch_request = datasource.get_asset("taxi_df").build_batch_request(dataframe=df)

# COMMAND ----------

suite_name

# COMMAND ----------

register new suite

# COMMAND ----------

validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)


# COMMAND ----------

 

# COMMAND ----------


expectation_json = {
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {
        "column": "trip_distance",
        "min_value": 0.1,
        "max_value": 100
    },
    "meta": {
        "created_by": "user@example.com",
        "created_at": "2025-04-30"

    }
}
new suite -> new suite
suite

# COMMAND ----------

validator.expect_column_values_to_not_be_null("tpep_pickup_datetime")
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="tpep_dropoff_datetime",
    column_B="tpep_pickup_datetime",
    or_equal=True  # allows dropoff == pickup
)
validator.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
validator.expect_column_values_to_be_between(
    column="trip_distance",
    min_value=0,
    strict_min=True  # ensures strictly greater than 0
)
validator.expect_column_values_to_be_in_set(
    "payment_type",
    value_set=[1, 2, 3, 4, 5, 6]  # values: 1=Credit card, 2=Cash, etc.
)


# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

#older version

suite_name = "taxi_expectations"
context.add_or_update_expectation_suite(suite_name)
# Get default pandas datasource
datasource = context.sources.pandas_default

# Register the DataFrame as an asset
asset = datasource.add_dataframe_asset(name="newdf", dataframe=df)

# Use the asset + suite to get a validator
batch_request = asset.build_batch_request()
validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)



validator.expect_column_values_to_not_be_null("tpep_pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
validator.expect_column_values_to_be_between("total_amount", min_value=0)

validator.save_expectation_suite()


# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name="people_blob_checkpoint",
    validator=validator,
)

results = checkpoint.run()

# COMMAND ----------

results

# COMMAND ----------

import great_expectations as gx
print(gx.__version__)


# COMMAND ----------

validator.head()