# Databricks notebook source
dbutils.secrets.list('blob-scope')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get('blob-scope','service-credential-project')

# COMMAND ----------

application_id="257f1bbb-d8da-46ed-9dc1-a417530e9324"
directory_id="654ad10e-3170-4844-bb2d-b2c7a9ae5a8d"
service_credential = dbutils.secrets.get('blob-scope-rawstorage123','service-credential-project')

spark.conf.set("fs.azure.account.auth.type.rawstorage123.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rawstorage123.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rawstorage123.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.rawstorage123.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rawstorage123.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://ingestion@rawstorage123.dfs.core.windows.net/"))

# COMMAND ----------

df = spark.read.csv("abfss://ingestion@rawstorage123.dfs.core.windows.net/sales_data.csv", header=True, inferSchema="true")
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap

df = df.withColumn("Product", initcap(df["Product"])).withColumn("Region", initcap(df["Region"]))
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap,col, abs, when

df=df.withColumn(
    "Quantity",when(col("quantity").isNull(), 0).otherwise(abs(col("quantity"))))
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

df=df.withColumn("UnitPrice",regexp_replace(col("Price Per Unit"), r"\$", ""))
df = df.drop("Price Per Unit")
display(df)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce
df= df.withColumn("TransactionDate", 
    coalesce(
        to_date(col("Transaction Date"), "yyyy-MM-dd"),
        to_date(col("Transaction Date"), "MM-dd-yyyy"),
        to_date(col("Transaction Date"), "yyyy-MM-dd"),
        to_date(col("Transaction Date"), "dd/MM/yyyy"),
        to_date(col("Transaction Date"), "MM/dd/yyyy")
    )
)
df = df.drop("Transaction Date")
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
df=df.withColumnRenamed("Transaction ID", "TransactionID").withColumnRenamed("Customer Name", "CustomerName")
display(df)

# COMMAND ----------

# Step 1: Write the DataFrame to the directory
df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("abfss://processed@rawstorage123.dfs.core.windows.net/processeddata")




# COMMAND ----------

# Step 2: Define the paths
source_path = "abfss://processed@rawstorage123.dfs.core.windows.net/processeddata/"
destination_path = "abfss://processed@rawstorage123.dfs.core.windows.net/processed_renamed/processed_sales_data.csv"

# Step 3: List the files in the source directory
files = dbutils.fs.ls(source_path)

# Step 4: Find the file that matches the pattern
source_file_path = None
for file in files:
    if "part-00000" in file.name:
        source_file_path = file.path
        break

# Step 5: Rename the part file to the desired name
if source_file_path:
    dbutils.fs.mv(source_file_path, destination_path)
else:
    raise FileNotFoundError("The specified file part-00000 was not found in the source directory.")
