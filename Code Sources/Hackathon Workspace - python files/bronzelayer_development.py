# Databricks notebook source
# MAGIC %md
# MAGIC # 🔵 Data Ingestion into Bronze Layer

# COMMAND ----------

# DBTITLE 1,Cell 1
# Bronze Layer Ingestion (Enterprise Clean Version)

from pyspark.sql.functions import current_timestamp, input_file_name
import re

catalog = "hackathon"
schema = "bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def clean_col(c):
    return re.sub('[^a-zA-Z0-9_]', '', c.strip().lower().replace(" ", "_"))

base_paths = [
    "/Volumes/hackathon/raw_data/datasets/SAP_IBP_RAW_DATA/",
    "/Volumes/hackathon/raw_data/datasets/SAP_RAW_DATA/"
]

for base_path in base_paths:
    for file in dbutils.fs.ls(base_path):
        if file.name.endswith(".csv"):
            
            table = file.name.replace(".csv", "").lower()
            
            df = (spark.read.format("csv")
                  .option("header", True)
                  .load(file.path))
            
            for c in df.columns:
                df = df.withColumnRenamed(c, clean_col(c))
            
            # Use input_file_name() to get source file path
            df = (df
                  .withColumn("ingestion_ts", current_timestamp()))
            
            (df.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{catalog}.{schema}.{table}"))
            
            print(f"Created {catalog}.{schema}.{table}")

print("Bronze ingestion completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧪 Bronze VALIDATION CHECK

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱Total table count & their Schema

# COMMAND ----------

spark.sql("SHOW TABLES IN hackathon.bronze").show(truncate=False)

# COMMAND ----------

tables = spark.sql("SHOW TABLES IN hackathon.bronze").collect()

for t in tables:
    print(f"\nTable: {t.tableName}")
    spark.sql(f"DESCRIBE TABLE hackathon.bronze.{t.tableName}").show(truncate=False)