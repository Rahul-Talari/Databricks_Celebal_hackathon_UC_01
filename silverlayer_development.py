# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER Layer NOTEBOOK – STEP-BY-STEP BUILD

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 0 — Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

catalog = "hackathon"
bronze_schema = "bronze"
silver_schema = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 1 — Build Helper Functions (Reusable)

# COMMAND ----------

def read_bronze(table_name: str) -> DataFrame:
    return spark.table(f"{catalog}.{bronze_schema}.{table_name}")

def write_silver(df: DataFrame, table_name: str):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.{silver_schema}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔵 INVENTORY DOMAIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 2 — silver.mara

# COMMAND ----------

mara = read_bronze("mara")

silver_mara = (
    mara
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("ersda", to_date(col("ersda"), "yyyyMMdd"))
    .dropDuplicates(["matnr"])
)

write_silver(silver_mara, "mara")
print("silver.mara built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 3 — silver.marc

# COMMAND ----------

marc = read_bronze("marc")

silver_marc = (
    marc
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werks", trim(col("werks")))
    .withColumn("plifz", col("plifz").cast("int"))
    .withColumn("webaz", col("webaz").cast("int"))
    .withColumn("eisbe", col("eisbe").cast("decimal(18,4)"))
    .withColumn("batch_management_flag",
                when(col("xchar") == "X", True).otherwise(False))
    .dropDuplicates(["matnr", "werks"])
)

write_silver(silver_marc, "marc")
print("silver.marc built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 4 — silver.mard (Non-batch stock)

# COMMAND ----------

mard = read_bronze("mard")

silver_mard = (
    mard
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werks", trim(col("werks")))
    .withColumn("lgort", trim(col("lgort")))
    .withColumn("labst", col("labst").cast("decimal(18,4)"))
    .withColumn("insme", col("insme").cast("decimal(18,4)"))
    .withColumn("speme", col("speme").cast("decimal(18,4)"))
    .dropDuplicates(["matnr", "werks", "lgort"])
)

write_silver(silver_mard, "mard")
print("silver.mard built")

# Grain: material + plant + storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 5 — silver.mchb (Batch stock)

# COMMAND ----------

mchb = read_bronze("mchb")

silver_mchb = (
    mchb
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werks", trim(col("werks")))
    .withColumn("lgort", trim(col("lgort")))
    .withColumn("charg", trim(col("charg")))
    .withColumn("clabs", col("clabs").cast("decimal(18,4)"))   # Unrestricted
    .withColumn("cinsm", col("cinsm").cast("decimal(18,4)"))   # Quality
    .withColumn("cspem", col("cspem").cast("decimal(18,4)"))   # Blocked
    .dropDuplicates(["matnr", "werks", "lgort", "charg"])
)

write_silver(silver_mchb, "mchb")
print("silver.mchb built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 6 — silver.march_agg (In-Transit)

# COMMAND ----------

march = read_bronze("march")

silver_march_agg = (
    march
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .groupBy("matnr", "werks", "lfgja", "lfmon")
    .agg(sum(col("trame").cast("decimal(18,4)")).alias("in_transit_qty"))
)

write_silver(silver_march_agg, "march_agg")
print("silver.march_agg built")

# Grain: material + plant + year + period

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 7 — silver.mbew (Valuation)

# COMMAND ----------

mbew = read_bronze("mbew")

silver_mbew = (
    mbew
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("stprs", col("stprs").cast("decimal(18,4)"))
    .withColumn("peinh", col("peinh").cast("decimal(18,4)"))
    .withColumn("effective_unit_cost",
                col("stprs") / col("peinh"))
    .dropDuplicates(["matnr", "bwkey"])
)

write_silver(silver_mbew, "mbew")
print("silver.mbew built")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🟢 PROCUREMENT DOMAIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 8.1 — silver.ekpo

# COMMAND ----------

ekpo = read_bronze("ekpo")

silver_ekpo = (
    ekpo
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("ebeln", trim(col("ebeln")))
    .withColumn("ebelp", trim(col("ebelp")))
    .withColumn("menge", col("menge").cast("decimal(18,4)"))
    .withColumn("netpr", col("netpr").cast("decimal(18,4)"))
    .withColumn("netwr", col("netwr").cast("decimal(18,4)"))
    .dropDuplicates(["ebeln", "ebelp"])
)

write_silver(silver_ekpo, "ekpo")
print("silver.ekpo built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 8.2 — silver._ekko

# COMMAND ----------

from pyspark.sql.functions import *

# Load bronze
ekko = spark.table("hackathon.bronze.ekko")

silver_ekko = (
    ekko
    .withColumn("ebeln", trim(col("ebeln")))
    .withColumn("lifnr", lpad(trim(col("lifnr")), 10, "0"))
    .withColumn("waers", trim(col("waers")))
    .withColumn("aedat", to_date(col("aedat"), "yyyyMMdd"))
    .dropDuplicates(["ebeln"])
)

(silver_ekko.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.silver.ekko"))

print("silver.ekko built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 9 — silver.eket_agg

# COMMAND ----------

eket = read_bronze("eket")

silver_eket_agg = (
    eket
    .groupBy("ebeln", "ebelp")
    .agg(
        sum(col("menge").cast("decimal(18,4)")).alias("scheduled_qty"),
        max("eindt").alias("next_schedule_date")
    )
)

write_silver(silver_eket_agg, "eket_agg")
print("silver.eket_agg built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 10 — silver.ekbe_agg

# COMMAND ----------

ekbe = read_bronze("ekbe")

silver_ekbe_agg = (
    ekbe
    .groupBy("ebeln", "ebelp")
    .agg(
        sum(col("menge").cast("decimal(18,4)")).alias("delivered_qty")
    )
)

write_silver(silver_ekbe_agg, "ekbe_agg")
print("silver.ekbe_agg built")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🟡 DEMAND DOMAIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 11.1 — silver.demand_actual

# COMMAND ----------

demand_actual = read_bronze("demand_actual")

silver_demand_actual = (
    demand_actual
    .withColumn("period_date",
        to_date(concat(col("period"), lit("01")), "yyyyMMdd"))
    .withColumn("actual_qty", col("actual_qty").cast("decimal(18,4)"))
    .withColumn("net_actual_qty", col("net_actual_qty").cast("decimal(18,4)"))
    .dropDuplicates(["period", "material", "market"])
)

write_silver(silver_demand_actual, "demand_actual")
print("silver.demand_actual built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 11.2 — silver.demand_forecast

# COMMAND ----------

from pyspark.sql.functions import *

# Load Bronze forecast table
forecast_bronze = spark.table("hackathon.bronze.demand_forcast")

# Build Silver version (FIXED period handling)
silver_forecast = (
    forecast_bronze
    .select(
        lpad(trim(col("material")), 18, "0").alias("material"),
        trim(col("market")).alias("market"),

        # Convert YYYYMM → proper date (first day of month)
        to_date(
            concat(col("period"), lit("01")),
            "yyyyMMdd"
        ).alias("period_date"),

        col("consensus_demand").cast("decimal(18,4)").alias("consensus_demand"),
        col("ibp_forecast").cast("decimal(18,4)").alias("ibp_forecast"),
        col("budget_volumes").cast("decimal(18,4)").alias("budget_volumes")
    )
    .dropDuplicates(["material", "market", "period_date"])
)

# Write to Silver
(silver_forecast.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.silver.demand_forecast"))

print("silver.demand_forecast built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🟣 QUALITY DOMAIN (Silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 12 — silver.qals (Inspection Lot Header)

# COMMAND ----------

qals = read_bronze("qals")

silver_qals = (
    qals
    .withColumn("prueflos", trim(col("prueflos")))
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werk", trim(col("werk")))
    .withColumn("charg", trim(col("charg")))
    .withColumn("losmenge", col("losmenge").cast("decimal(18,4)"))
    .withColumn("lmengepr", col("lmengepr").cast("decimal(18,4)"))
    .withColumn("lmengeist", col("lmengeist").cast("decimal(18,4)"))
    .withColumn("lmengesch", col("lmengesch").cast("decimal(18,4)"))
    .withColumn("ersteldat", to_date(col("ersteldat"), "yyyyMMdd"))
    .dropDuplicates(["prueflos"])
)

write_silver(silver_qals, "qals")
print("silver.qals built")

# Grain: 1 row per PRUEFLOS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 13 — silver.qave_agg (Usage Decision Aggregated)

# COMMAND ----------

qave = read_bronze("qave")

silver_qave_agg = (
    qave
    .groupBy("prueflos")
    .agg(
        max("vcode").alias("ud_code"),
        max(to_date(col("vdatum"), "yyyyMMdd")).alias("ud_date")
    )
)

write_silver(silver_qave_agg, "qave_agg")
print("silver.qave_agg built")

# This prevents row multiplication later.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔵 HISTORICAL INVENTORY DOMAIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 14 — silver.mardh (Historical Non-Batch Stock)

# COMMAND ----------

mardh = read_bronze("mardh")

silver_mardh = (
    mardh
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werks", trim(col("werks")))
    .withColumn("lgort", trim(col("lgort")))
    .withColumn("lfgja", col("lfgja").cast("int"))
    .withColumn("lfmon", col("lfmon").cast("int"))
    .withColumn("labst", col("labst").cast("decimal(18,4)"))
    .withColumn("insme", col("insme").cast("decimal(18,4)"))
    .withColumn("speme", col("speme").cast("decimal(18,4)"))
    .dropDuplicates(["matnr", "werks", "lgort", "lfgja", "lfmon"])
)

write_silver(silver_mardh, "mardh")
print("silver.mardh built")

# Grain: MATNR + WERKS + LGORT + LFGJA + LFMON

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 15 — silver.mchbh (Historical Batch Stock)

# COMMAND ----------

mchbh = read_bronze("mchbh")

silver_mchbh = (
    mchbh
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("werks", trim(col("werks")))
    .withColumn("lgort", trim(col("lgort")))
    .withColumn("charg", trim(col("charg")))
    .withColumn("lfgja", col("lfgja").cast("int"))
    .withColumn("lfmon", col("lfmon").cast("int"))
    .withColumn("clabs", col("clabs").cast("decimal(18,4)"))
    .withColumn("cinsm", col("cinsm").cast("decimal(18,4)"))
    .dropDuplicates(["matnr", "werks", "lgort", "charg", "lfgja", "lfmon"])
)

write_silver(silver_mchbh, "mchbh")
print("silver.mchbh built")

# Grain: MATNR + WERKS + LGORT + CHARG + LFGJA + LFMON

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 STEP 16 — silver.mbewh (Historical Valuation)

# COMMAND ----------

mbewh = read_bronze("mbewh")

silver_mbewh = (
    mbewh
    .withColumn("matnr", lpad(trim(col("matnr")), 18, "0"))
    .withColumn("lfgja", col("lfgja").cast("int"))
    .withColumn("lfmon", col("lfmon").cast("int"))
    .withColumn("stprs", col("stprs").cast("decimal(18,4)"))
    .withColumn("verpr", col("verpr").cast("decimal(18,4)"))
    .withColumn("peinh", col("peinh").cast("decimal(18,4)"))
    .dropDuplicates(["matnr", "bwkey", "lfgja", "lfmon"])
)

write_silver(silver_mbewh, "mbewh")
print("silver.mbewh built")

# COMMAND ----------

# MAGIC %md
# MAGIC # Snapshot Table

# COMMAND ----------

from pyspark.sql.functions import *

forecast_bronze = spark.table("hackathon.bronze.demand_forecastt")

silver_snapshot = (
    forecast_bronze
    .select(
        lpad(trim(col("material")), 18, "0").alias("material"),
        trim(col("market")).alias("market"),
        to_date(concat(col("target_period"), lit("01")), "yyyyMMdd").alias("target_date"),
        to_date(concat(col("snapshot_period"), lit("01")), "yyyyMMdd").alias("snapshot_date"),
        col("consensus_forecast").cast("decimal(18,4)").alias("forecast_consensus")
    )
)

display(silver_snapshot)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧪 SILVER VALIDATION CHECK

# COMMAND ----------

for t in spark.sql("SHOW TABLES IN hackathon.silver").collect():
    table_name = t.tableName
    print(f"\nValidating {table_name}")
    
    df = spark.table(f"hackathon.silver.{table_name}")
    print("Row Count:", df.count())

# COMMAND ----------

