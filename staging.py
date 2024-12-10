# Databricks notebook source
application_id="257f1bbb-d8da-46ed-9dc1-a417530e9324"
directory_id="654ad10e-3170-4844-bb2d-b2c7a9ae5a8d"
service_credential = dbutils.secrets.get('blob-scope-rawstorage123','service-credential-project')

spark.conf.set("fs.azure.account.auth.type.rawstorage123.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rawstorage123.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rawstorage123.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.rawstorage123.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rawstorage123.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@rawstorage123.dfs.core.windows.net/processed_renamed"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

sales_schema = StructType([
    StructField("TransactionID",StringType(), True),
    StructField("CustomerName",StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity",DoubleType(), True),
    StructField("Region", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("TransactionDate", DateType(), True),
    StructField("TotalAmount", IntegerType(), True),
])

df = spark.read.csv(
    "abfss://processed@rawstorage123.dfs.core.windows.net/processed_renamed/processed_sales_data.csv",
    header=True,
    schema=sales_schema
)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# Clean Price Per Unit and derive Total Amount
df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

#df.withColumn("TransactionDate", current_timestamp()).write.mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").format("delta").save("abfss://staging@rawstorage123.dfs.core.windows.net/processed_sales_data").show()

df = df.withColumn("TimeStamp", current_timestamp())
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Partition on Region

# COMMAND ----------

from pyspark.sql.functions import expr, year,col

df = df.withColumn("DiscountedAmount", col("TotalAmount") * 0.9)

df = df.filter(col("Quantity") > 0)

# Add Transaction Year
df = df.withColumn("TransactionYear", year(col("TransactionDate")))

df.write.mode("overwrite").partitionBy("Region").format("csv").option("header", "true").save("abfss://staging@rawstorage123.dfs.core.windows.net/processed_sales_data.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# Write the DataFrame to Delta format with schema merge enabled
df.write.mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").format("delta").save("abfss://staging@rawstorage123.dfs.core.windows.net/processed_sales_data/")

df.show()
#df.write.mode("overwrite").mode("overwrite").format("delta").save("abfss://staging@rawstorage123.dfs.core.windows.net/processed_sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

#from delta.tables import DeltaTable

#existing_data = DeltaTable.forPath(spark, "abfss://staging@rawstorage123.dfs.core.windows.net/processed_sales_data/")

#existing_data.alias("existing").merge(df.alias("new"),"existing.TransactionID=new.TransactionID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()