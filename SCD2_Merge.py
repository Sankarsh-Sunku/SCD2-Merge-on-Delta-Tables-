# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
import os

# sparkversion need to add while creating the compute
SPARK_VERSION = 3.5

# COMMAND ----------

# print(os.environ['SPARK_VERSION']) #This need to be configured while creating the compute 

# COMMAND ----------

date = dbutils.widgets.get("arrival_date")

booking_data = f"/Volumes/transaction_catalog/default/bookingdata/bookings_{date}.csv"
customer_data = f"/Volumes/transaction_catalog/default/customerdata/customers_{date}.csv"
print(booking_data)
print(customer_data)

booking_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", "\"") \
    .option("multiLine", "true")\
    .load(booking_data)

booking_df.printSchema()
display(booking_data)

customer_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", "\"")\
    .option("multiLine", "true")\
    .load(customer_data)

customer_df.printSchema()
display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Check(spark, CheckLevel.Error, "Booking Data Check"):
# MAGIC
# MAGIC Initializes a check named "Booking Data Check".
# MAGIC CheckLevel.Error: This defines the severity level of the check. If any of the conditions fail, it will raise an error.
# MAGIC .hasSize(lambda x: x > 0):
# MAGIC
# MAGIC This ensures that the dataset has more than 0 records (i.e., it is not empty). x is the number of rows, and the condition x > 0 must be true.
# MAGIC .isUnique("booking_id", hint="Booking ID is not unique throughout"):
# MAGIC
# MAGIC This checks that the values in the booking_id column are unique. If any duplicate values are found, it triggers an error and the provided hint "Booking ID is not unique throughout" will be displayed.
# MAGIC .isComplete("customer_id"):
# MAGIC
# MAGIC This verifies that the customer_id column has no missing (null) values. Every customer_id must be present in each record.
# MAGIC .isComplete("amount"):
# MAGIC
# MAGIC Similar to the previous check, this ensures the amount column has no missing values.
# MAGIC .isNonNegative("amount"):
# MAGIC
# MAGIC This checks that the values in the amount column are non-negative (i.e., greater than or equal to 0).
# MAGIC .isNonNegative("quantity"):
# MAGIC
# MAGIC Similarly, this ensures that the quantity column only contains non-negative values.
# MAGIC .isNonNegative("discount"):
# MAGIC
# MAGIC This ensures that the discount column only contains non-negative values.

# COMMAND ----------

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult


check_booking_data = Check(spark, CheckLevel.Error, "Booking Data Check")\
    .hasSize(lambda x: x > 0)\
    .isUnique("booking_id", hint="Booking ID is not unique throught")\
    .isComplete("customer_id")\
    .isComplete("amount")\
    .isNonNegative("amount")\
    .isNonNegative("quantity")\
    .isNonNegative("discount")


check_scd = Check(spark, CheckLevel.Error, "Customer Data Check") \
    .hasSize(lambda x: x > 0) \
    .isUnique("customer_id") \
    .isComplete("customer_name") \
    .isComplete("customer_address") \
    .isComplete("email")

booking_dq_check = VerificationSuite(spark) \
    .onData(booking_df) \
    .addCheck(check_booking_data) \
    .run()

customer_dq_check = VerificationSuite(spark) \
    .onData(customer_df) \
    .addCheck(check_scd) \
    .run()

booking_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, booking_dq_check)
display(booking_dq_check_df)

customer_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, customer_dq_check)
display(customer_dq_check_df)

if booking_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Booking Data")

if customer_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Customer Data")

# COMMAND ----------

# Add ingestion timestamp to booking data
booking_df_incremental = booking_df.withColumn("ingestion_time", current_timestamp())

# Join booking data with customer data
df_joined = booking_df_incremental.join(customer_df, "customer_id")

df_transformed = df_joined \
    .withColumn("total_cost", col("amount") - col("discount")) \
    .filter(col("quantity") > 0)

df_transformed_agg = df_transformed \
    .groupBy("booking_type", "customer_id") \
    .agg(
        sum("total_cost").alias("total_amount_sum"),
        sum("quantity").alias("total_quantity_sum")
    )

fact_table_path = "transaction_catalog.default.fact_table"
fact_table_exists = spark._jsparkSession.catalog().tableExists(fact_table_path)

if fact_table_exists:
    df_existing_fact = spark.read.format("delta").table(fact_table_path)

    df_combined = df_existing_fact.unionByName(df_transformed_agg, allowMissingColumns=True)
    
    # Perform another group by and aggregation on the combined data
    df_final_agg = df_combined \
        .groupBy("booking_type", "customer_id") \
        .agg(
            sum("total_amount_sum").alias("total_amount_sum"),
            sum("total_quantity_sum").alias("total_quantity_sum")
        )
else:
    df_final_agg = df_transformed_agg

display(df_final_agg)

df_final_agg.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(fact_table_path)

scd_table_path = "incremental_load.default.customer_dim"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

if scd_table_exists:
    scd_table = DeltaTable.forName(spark,scd_table_path)
    display(scd_table.toDF())

    scd_table.alias("scd").merge(
        customer_df.alias("source"), "scd.customer_id = source.customer_id and scd.valid_to = '9999-12-31'"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
        }) \
        .execute()

    customer_df.write.format("delta").mode("append").saveAsTable(scd_table_path)
else:
    customer_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(scd_table_path)

# COMMAND ----------



# COMMAND ----------


