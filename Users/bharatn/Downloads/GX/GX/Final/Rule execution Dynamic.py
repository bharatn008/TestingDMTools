# Databricks notebook source
# csv - /databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz
# delta - dbfs:/GXtestingdata/taxi

# COMMAND ----------

dbutils.widgets.text("suite_name", "taxi_expectations")
dbutils.widgets.text("table_name", "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
dbutils.widgets.text("asset_name", "taxi_asset")
dbutils.widgets.text("checkpoint_name", "taxi_checkpoint")
# dbutils.widgets.text("output_path", "/mnt/validated_results/output.json")
dbutils.widgets.text("catalog_name", "hive_metastore")      # e.g., "main"
dbutils.widgets.text("schema_name", "default")        # e.g., "dq_logs"
dbutils.widgets.text("output_table_name", "taxi_expectations")    


# COMMAND ----------

# DBTITLE 1,Entry point
suite_name = dbutils.widgets.get("suite_name")
table_name = dbutils.widgets.get("table_name")
asset_name = dbutils.widgets.get("asset_name")
checkpoint_name = dbutils.widgets.get("checkpoint_name")
# output_path = dbutils.widgets.get("output_path")

catalog_name = dbutils.widgets.get("catalog_name")      # e.g., "main"
schema_name = dbutils.widgets.get("schema_name")        # e.g., "dq_logs"
output_table_name = dbutils.widgets.get("output_table_name")    

# COMMAND ----------

print("==== Parameter Crosscheck ====")
print(f"suite_name        : {suite_name}")
print(f"table_name        : {table_name}")
print(f"asset_name        : {asset_name}")
print(f"checkpoint_name   : {checkpoint_name}")
# print(f"output_path       : {output_path}")
print(f"catalog_name      : {catalog_name}")
print(f"schema_name       : {schema_name}")
print(f"output_table_name : {output_table_name}")
print("================================")


# COMMAND ----------

import great_expectations as gx

# 1. Initialize GX manually
context = gx.data_context.FileDataContext.create(
    project_root_dir="/dbfs/mnt/gxtesting/great_expectations"
)

print("GX context created at /mnt/gxtesting/great_expectations")


# COMMAND ----------

context.run_checkpoint(
    checkpoint_name="taxi_checkpoint",
    batch_request=batch_request
)


# COMMAND ----------

print("Available datasources:", list(context.datasources.keys()))
print("Available expectation suites:", context.list_expectation_suite_names())

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt")

# COMMAND ----------

# assistant_result = context.assistants.onboarding.run(batch_request=batch_request)#ds5 multi core: 2-5 took ? min

# COMMAND ----------

assistant_result = context.assistants.onboarding.run(batch_request=batch_request)#ds5 - 30 min

# COMMAND ----------

# DBTITLE 1,Data Profiling
assistant_result = context.assistants.onboarding.run(batch_request=batch_request) #DS3

# COMMAND ----------

# context.save_expectation_suite(
#     expectation_suite=assistant_result.get_expectation_suite(),
#     expectation_suite_name="profiling_suite"
# )

# COMMAND ----------

# validator = context.get_validator(
#     batch_request=batch_request,
#     expectation_suite_name="profiling_suite"
# )


# COMMAND ----------

# import pandas
# df = pandas.read_csv(table_name)


# COMMAND ----------

# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

# COMMAND ----------

# DBTITLE 1,Read delta table
df_delta = spark.read.format("delta").load(table_name)

# COMMAND ----------

# df_delta.count()

# COMMAND ----------

# DBTITLE 1,creating pandas dataset
# # Get the pandas default datasource
# datasource = context.sources.pandas_default

# # Register the new DataFrame as an asset
# batch_request = datasource.get_asset(asset_name).build_batch_request(dataframe=df)


# COMMAND ----------

# DBTITLE 1,spark dataset using delta table
# Get the Spark default datasource
datasource = context.sources.add_or_update_spark("testingdelta")

# Register the DataFrame as an in-memory asset (temporary)
asset = datasource.add_dataframe_asset(name= asset_name)

# Build the batch request from the Spark DataFrame
batch_request = asset.build_batch_request(dataframe=df_delta)

# COMMAND ----------

# DBTITLE 1,addded this class as this is custom rule suite

#we need to run this block whenever we need to run this custom rule
from great_expectations.expectations.expectation import QueryExpectation
from great_expectations.execution_engine import SparkDFExecutionEngine, PandasExecutionEngine

class ExpectNoInvalidTripOrPayment(QueryExpectation):
    metric_dependencies = ()
    success_keys = ("query",)

    def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
        query = self.get_success_kwargs(configuration).get("query")
        if not query:
            raise ValueError("Missing required `query` parameter.")

        if isinstance(execution_engine, SparkDFExecutionEngine):
            batch_data = execution_engine.dataframe
            batch_data.createOrReplaceTempView("temp_trip_data")
            formatted_query = query.format(active_batch="temp_trip_data")
            result = execution_engine.spark.sql(formatted_query).collect()[0]
            total_invalids = result["total_invalids"]
            invalid_payment_type = result["invalid_payment_type"]

        elif isinstance(execution_engine, PandasExecutionEngine):
            batch_data = execution_engine.dataframe
            invalid_trip_distance = batch_data[batch_data["trip_distance"] <= 0].shape[0]
            invalid_payment_type = batch_data[~batch_data["payment_type"].isin(['1', '2', '3'])].shape[0]
            total_invalids = invalid_trip_distance + invalid_payment_type

        else:
            raise ValueError("Unsupported execution engine.")

        success = total_invalids == 0 and invalid_payment_type == 0
        return {
            "success": success,
            "result": {
                "observed_value": {
                    "total_invalids": total_invalids,
                    "invalid_payment_type": invalid_payment_type,
                }
            }
        }


from great_expectations.expectations.registry import get_expectation_impl

# Dynamically register it
get_expectation_impl("expect_no_invalid_trip_or_payment").register(ExpectNoInvalidTripOrPayment)  

# COMMAND ----------

suite_name

# COMMAND ----------

# Use the existing suite and batch request
validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# COMMAND ----------

# import time

# # Start timer
# start_time = time.time()

# # Run the suite
# results = validator.validate()

# # End timer
# end_time = time.time()

# # Time taken
# duration = end_time - start_time
# print(f"Time taken to run expectation suite: {duration:.2f} seconds")


# COMMAND ----------

validator.expectation_suite_name

# COMMAND ----------

checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "Checkpoint",
    "validations": [
        {
            "batch_request": validator.active_batch.batch_request,
            "expectation_suite_name": validator.expectation_suite_name,
        }
    ],
    "action_list": [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"}
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"}
        },
        {
            "name": "send_email_on_validation",
            "action": {
                "class_name": "EmailAction",
                "notify_on": "all",
                "notify_with": ["validation_result"],
                "renderer": {
                    "module_name": "great_expectations.render.renderer.notification_renderer",
                    "class_name": "EmailRenderer"
                },
                "smtp_address": "${smtp_address}",
                "smtp_port": "${smtp_port}",
                "sender_login": "${sender_login}",
                "sender_password": "${sender_password}",
                "receiver_emails": "${receiver_emails}"
            }
        }
    ]
}

context.add_or_update_checkpoint(**checkpoint_config)


# COMMAND ----------

checkpoint

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    validator=validator,
)


# COMMAND ----------

checkpoint_name

# COMMAND ----------

context.run_checkpoint(name=checkpoint_name)

# COMMAND ----------

import time

# Start timer
start_time = time.time()
# Run the suite
results = checkpoint.run()
# End timer
end_time = time.time()
# Time taken
duration = end_time - start_time
print(f"Time taken to run expectation suite: {duration:.2f} seconds")


# COMMAND ----------

results

# COMMAND ----------

checkpoint

# COMMAND ----------

# DBTITLE 1,Parse results into DF
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col

result_rows = []

for key, result_data in results["run_results"].items():
    validation = result_data["validation_result"]
    suite_name = validation["meta"].get("expectation_suite_name")
    checkpoint_name = validation["meta"].get("checkpoint_name")
    validation_time = validation["meta"].get("validation_time")

    for res in validation["results"]:
        expectation = res["expectation_config"]
        result_detail = res["result"]

        # Convert list to strings for Spark compatibility
        partial_unexpected_list = result_detail.get("partial_unexpected_list")
        if partial_unexpected_list is None:
            partial_unexpected_list = []
        elif not isinstance(partial_unexpected_list, list):
            partial_unexpected_list = [str(partial_unexpected_list)]
        else:
            partial_unexpected_list = [str(x) for x in partial_unexpected_list]

        result_rows.append({
            "expectation_suite_name": suite_name,
            "checkpoint_name": checkpoint_name,
            "validation_time": validation_time,
            "expectation_type": expectation["expectation_type"],
            "column": expectation["kwargs"].get("column", None),
            "success": res["success"],
            "element_count": result_detail.get("element_count"),
            "unexpected_count": result_detail.get("unexpected_count"),
            "unexpected_percent": result_detail.get("unexpected_percent"),
            "partial_unexpected_list": partial_unexpected_list,
        })

# Create Pandas and Spark DataFrames
result_df_pd = pd.DataFrame(result_rows)
spark = SparkSession.builder.getOrCreate()
result_df_spark = spark.createDataFrame(result_df_pd)
from pyspark.sql.functions import  current_timestamp, from_utc_timestamp, date_format
# Optional: explicitly cast column to avoid further issues
result_df_spark = result_df_spark.withColumn("partial_unexpected_list", col("partial_unexpected_list").cast(ArrayType(StringType()))).withColumn(
    "current_time_IST",
    date_format(from_utc_timestamp(current_timestamp(), "Asia/Kolkata"), "yyyy-MM-dd HH:mm:ss")
).drop("validation_time")

# COMMAND ----------

display(result_df_spark)

# COMMAND ----------

# DBTITLE 1,Clean df and write into Catalog

full_table_name = f"{catalog_name}.{schema_name}.{output_table_name}"
result_df_spark.write.option("mergeSchema", "true").mode("append").saveAsTable(full_table_name)

# COMMAND ----------

import great_expectations as gx
print(gx.__version__)
