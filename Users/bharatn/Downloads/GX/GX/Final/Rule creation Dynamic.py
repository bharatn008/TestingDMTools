# Databricks notebook source
# Will get array of expectations from UI

# COMMAND ----------

import json
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

# DBTITLE 1,get list of all ex from site
# from great_expectations.expectations.registry import _registered_expectations
# import inspect
# import json

# expectation_info = {}

# for exp_type, exp_class in _registered_expectations.items():
#     try:
#         docstring = inspect.getdoc(exp_class) or ""
#         expectation_info[exp_type] = {
#             "description": docstring
#         }
#     except Exception as e:
#         expectation_info[exp_type] = {
#             "description": "No doc available"
#         }




# COMMAND ----------

dbutils.widgets.text("suite_name", "taxi_expectations")
dbutils.widgets.text("table_name", "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")
dbutils.widgets.text("asset_name", "taxi_asset")
dbutils.widgets.text("checkpoint_name", "taxi_checkpoint")
# Create a text widget for the entire expectation JSON
dbutils.widgets.text("rule_json", """{
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
}""", "Expectation Rule JSON")

dbutils.widgets.text("catalog_name", "hive_metastore")      # e.g., "main"
dbutils.widgets.text("schema_name", "default")        # e.g., "dq_logs"
dbutils.widgets.text("output_table_name", "taxi_expectations")    
3


# COMMAND ----------

suite_name = dbutils.widgets.get("suite_name")
table_name = dbutils.widgets.get("table_name")
asset_name = dbutils.widgets.get("asset_name")
checkpoint_name = dbutils.widgets.get("checkpoint_name")
rule_json = json.loads(dbutils.widgets.get("rule_json"))     
catalog_name = dbutils.widgets.get("catalog_name")     
schema_name = dbutils.widgets.get("schema_name")      
output_table_name = dbutils.widgets.get("output_table_name")    

# COMMAND ----------

rule_json

# COMMAND ----------

# df = read as delta

# COMMAND ----------

import pandas
df = pandas.read_csv(table_name)


# COMMAND ----------

import great_expectations as gx

# 1. Initialize GX manually
context = gx.data_context.FileDataContext.create(
    project_root_dir="/dbfs/mnt/gxtesting/great_expectations"
)

print("GX context created at /mnt/gxtesting/great_expectations")


# COMMAND ----------

print("Available datasources:", list(context.datasources.keys()))
print("Available expectation suites:", context.list_expectation_suite_names())

# COMMAND ----------

try:
    suite = context.get_expectation_suite(suite_name)
except Exception:
    print('new')
    suite = context.add_expectation_suite(suite_name)


# COMMAND ----------

# DBTITLE 1,pandas ds
# Get the pandas default datasource
datasource = context.sources.pandas_default

# Register the new DataFrame as an asset
batch_request = datasource.get_asset(asset_name).build_batch_request(dataframe=df)


# COMMAND ----------

# DBTITLE 1,spark ds
# Get the Spark default datasource
datasource = context.sources.add_or_update_spark("testingdelta")

# Register the DataFrame as an in-memory asset (temporary)
asset = datasource.add_dataframe_asset(name= asset_name)

# Build the batch request from the Spark DataFrame
batch_request = asset.build_batch_request(dataframe=df_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# COMMAND ----------

# DBTITLE 1,add all rules from JSON
expectations = rule_json.get("expectations", [])
for expectation in expectations:
    print(expectation)
    expectation_type = expectation["expectation_type"]
    kwargs = expectation.get("kwargs", {})
    meta = expectation.get("meta", {})

    # Dynamically call the method on validator
    expectation_method = getattr(validator, expectation_type)
    expectation_result = expectation_method(**kwargs, meta=meta)

# COMMAND ----------

# # Parse expectation_type and kwargs
# expectation_type = rule_json["expectation_type"]
# kwargs = rule_json.get("kwargs", {})
# meta = rule_json.get("meta", {})

# # Dynamically call the method on validator
# expectation_method = getattr(validator, expectation_type)
# expectation_result = expectation_method(**kwargs, meta=meta)


# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    validator=validator,
)

# COMMAND ----------

res = context.run_checkpoint(checkpoint_name=checkpoint_name, run_name="testingcustom")

# COMMAND ----------

res

# COMMAND ----------

# MAGIC %md
# MAGIC Adding custom rule

# COMMAND ----------

spark_df = spark.createDataFrame(df)
# As we are executing SQL, we need spark df instead of pandas df which is default.

# COMMAND ----------

# spark_df.limit(500).createOrReplaceTempView("taxi_temp_view")

# COMMAND ----------

# from great_expectations.expectations.expectation import QueryExpectation
# from great_expectations.execution_engine import SparkDFExecutionEngine

# class ExpectNoInvalidTripOrPayment(QueryExpectation):
#     metric_dependencies = ()
#     success_keys = ("mostly",)

#     query = my_query

#     def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
#         mostly = self.get_success_kwargs(configuration).get("mostly", 1.0)
#         active_batch_name = execution_engine.active_batch_data.selectable.name
#         sql = self.query.format(active_batch=active_batch_name)

#         result = execution_engine.engine.sql(sql).collect()[0]
#         total_invalids = result["total_invalids"]

#         return {
#             "success": total_invalids <= 0 if mostly == 1.0 else total_invalids <= (1 - mostly),
#             "result": {
#                 "observed_value": {
#                     "total_invalids": total_invalids,
#                     "invalid_payment_type": result["invalid_payment_type"],
#                 }
#             },
#         }


# COMMAND ----------

# DBTITLE 1,sql rule, which will take from UI
mysql = """
    SELECT 
        COUNT(*) AS total_invalids,
        SUM(CASE WHEN payment_type NOT IN ('1', '2', '3') THEN 1 ELSE 0 END) AS invalid_payment_type
    FROM {active_batch}
    WHERE trip_distance <= 0 OR payment_type NOT IN ('1', '2', '3') limit 1000
    """

# COMMAND ----------

# DBTITLE 1,Custom logic on Spark df class register - with query as param

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

# DBTITLE 1,Custom logic on Spark df class register
# #another spark try
# from great_expectations.expectations.expectation import QueryExpectation
# from great_expectations.execution_engine import SparkDFExecutionEngine, PandasExecutionEngine
# # import pandasql

# class ExpectNoInvalidTripOrPayment(QueryExpectation):
#     # query = """
#     # SELECT 
#     #     COUNT(*) AS total_invalids,
#     #     SUM(CASE WHEN payment_type NOT IN ('1', '2', '3') THEN 1 ELSE 0 END) AS invalid_payment_type
#     # FROM {active_batch}
#     # WHERE trip_distance <= 0 OR payment_type NOT IN ('1', '2', '3')
#     # """
#     # Define constructor (optional)
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     # Define the validate method
#     def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
        
#         mostly = self.get_success_kwargs(configuration).get("mostly", 1.0)

#         batch_data = execution_engine.dataframe
#         batch_data.createOrReplaceTempView("active_batch")

#         sql = self.query.format(active_batch="active_batch")
#         result = execution_engine.spark.sql(sql).collect()[0]

#         total_invalids = result["total_invalids"]
#         invalid_payment_type = result["invalid_payment_type"]
#         # Check if the invalid percentage is less than `mostly`
#         success = total_invalids == 0 and invalid_payment_type == 0
#         return {
#             "success": success,
#             "result": {
#                 "total_invalids": result,
#                 "invalid_payment_type": invalid_payment_type
#             }
#         }

# # Dynamically register the custom expectation
# # get_expectation_impl("expect_no_invalid_trip_or_payment").register(ExpectNoInvalidTripOrPayment)


# COMMAND ----------

# #using sql query


# # custom_expectations/expect_no_invalid_trip_or_payment.py
# from great_expectations.expectations.expectation import QueryExpectation
# from great_expectations.execution_engine import SparkDFExecutionEngine, PandasExecutionEngine
# import pandasql

# class ExpectNoInvalidTripOrPayment(QueryExpectation):
#     query = """
#     SELECT 
#         COUNT(*) AS total_invalids,
#         SUM(CASE WHEN payment_type NOT IN ('1', '2', '3') THEN 1 ELSE 0 END) AS invalid_payment_type
#     FROM {active_batch}
#     WHERE trip_distance <= 0 OR payment_type NOT IN ('1', '2', '3')
#     """

#     def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
#         mostly = self.get_success_kwargs(configuration).get("mostly", 1.0)

#         if isinstance(execution_engine, SparkDFExecutionEngine):
#             batch_data = execution_engine.dataframe
#             batch_data.createOrReplaceTempView("active_batch")

#             sql = self.query.format(active_batch="active_batch")
#             result = execution_engine.engine.sql(sql).collect()[0]

#             total_invalids = result["total_invalids"]
#             invalid_payment_type = result["invalid_payment_type"]

#         elif isinstance(execution_engine, PandasExecutionEngine):
#             batch_data = execution_engine.dataframe
#             sql = self.query.format(active_batch="batch_data")
#             result_df = pandasql.sqldf(sql, locals())

#             if result_df.empty:
#                 total_invalids = 0
#                 invalid_payment_type = 0
#             else:
#                 total_invalids = result_df["total_invalids"].iloc[0]
#                 invalid_payment_type = result_df["invalid_payment_type"].iloc[0]

#         else:
#             raise ValueError("Unsupported execution engine.")

#         return {
#             "success": total_invalids <= 0 if mostly == 1.0 else total_invalids <= (1 - mostly),
#             "result": {
#                 "observed_value": {
#                     "total_invalids": total_invalids,
#                     "invalid_payment_type": invalid_payment_type,
#                 }
#             },
#         }


# COMMAND ----------

# from great_expectations.expectations.expectation import QueryExpectation
# from great_expectations.execution_engine import SparkDFExecutionEngine, PandasExecutionEngine

# class ExpectNoInvalidTripOrPayment(QueryExpectation):
#     metric_dependencies = ()
#     success_keys = ("mostly",)

#     query = """
#     SELECT
#         COUNT(*) AS total_invalids,
#         SUM(CASE WHEN payment_type NOT IN ('1', '2', '3') THEN 1 ELSE 0 END) AS invalid_payment_type
#     FROM {active_batch}
#     WHERE trip_distance <= 0 OR payment_type NOT IN ('1', '2', '3')
#     """

#     def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
#         # Get "mostly" value for success criteria
#         mostly = self.get_success_kwargs(configuration).get("mostly", 1.0)

#         # Handle case for SparkExecutionEngine
#         if isinstance(execution_engine, SparkDFExecutionEngine):
#             print("SparkExecutionEngine")
#             # Access the active batch DataFrame
#             batch_data = execution_engine.dataframe

#             # Register the temporary view of the Spark DataFrame
#             batch_data.createOrReplaceTempView("temp_trip_data")

#             # Format the SQL query with the temporary view name
#             sql = self.query.format(active_batch="temp_trip_data")

#             # Execute the SQL query
#             result = execution_engine.spark.sql(sql).collect()[0]
#             total_invalids = result["total_invalids"]

#         # Handle case for PandasExecutionEngine
#         elif isinstance(execution_engine, PandasExecutionEngine):
#             print(execution_engine, PandasExecutionEngine)
#             # Access the batch data (Pandas DataFrame)
#             batch_data = execution_engine.dataframe

#             # Perform checks directly on the Pandas DataFrame
#             invalid_trip_distance = batch_data[batch_data["trip_distance"] <= 0].shape[0]
#             invalid_payment_type = batch_data[~batch_data["payment_type"].isin(['1', '2', '3'])].shape[0]

#             # Total invalids as the sum of both checks
#             total_invalids = invalid_trip_distance + invalid_payment_type

#         else:
#             raise ValueError("Unsupported execution engine.")

#         # Return the validation result based on the "mostly" threshold
#         return {
#             "success": total_invalids <= 0 if mostly == 1.0 else total_invalids <= (1 - mostly),
#             "result": {
#                 "observed_value": {
#                     "total_invalids": total_invalids,
#                     "invalid_payment_type": invalid_payment_type if isinstance(execution_engine, PandasExecutionEngine) else result["invalid_payment_type"],
#                 }
#             },
#         }


# COMMAND ----------

# DBTITLE 1,register expectation
# from custom_expectations.expect_no_invalid_trip_or_payment import ExpectNoInvalidTripOrPayment
from great_expectations.expectations.registry import get_expectation_impl

# Dynamically register it
get_expectation_impl("expect_no_invalid_trip_or_payment").register(ExpectNoInvalidTripOrPayment)  


# COMMAND ----------

# DBTITLE 1,call expectation and see results
result = validator.expect_no_invalid_trip_or_payment(query=mysql)
display(result)
