# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer NOTEBOOK – STEP-BY-STEP BUILD

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 STEP 0 — Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

catalog = "hackathon"
silver_schema = "silver"
gold_schema = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

# Helper functions:
def read_silver(table_name: str):
    return spark.table(f"{catalog}.{silver_schema}.{table_name}")

def write_gold(df, table_name: str):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.{gold_schema}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 Dimensional Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 1 — Build dim_date (Shared Across Galaxy)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define date range
start_date = "2015-01-01"
end_date = "2035-12-31"

# Generate continuous date sequence
date_df = (
    spark.range(1)
    .select(
        explode(
            sequence(
                to_date(lit(start_date)),
                to_date(lit(end_date)),
                expr("interval 1 day")
            )
        ).alias("calendar_date")
    )
)

# Build date dimension attributes
dim_date = (
    date_df
    .withColumn("date_key", date_format(col("calendar_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("calendar_date")))
    .withColumn("month_number", month(col("calendar_date")))
    .withColumn("month_name", date_format(col("calendar_date"), "MMMM"))
    .withColumn("quarter", quarter(col("calendar_date")))
    .withColumn("day_of_month", dayofmonth(col("calendar_date")))
    .withColumn("week_of_year", weekofyear(col("calendar_date")))
    .withColumn("is_month_end", last_day(col("calendar_date")) == col("calendar_date"))
)

# Create default row safely
default_row = (
    dim_date.limit(0)
    .withColumn("date_key", lit(-1))
    .withColumn("calendar_date", lit(None).cast("date"))
    .withColumn("year", lit(None).cast("int"))
    .withColumn("month_number", lit(None).cast("int"))
    .withColumn("month_name", lit("UNKNOWN"))
    .withColumn("quarter", lit(None).cast("int"))
    .withColumn("day_of_month", lit(None).cast("int"))
    .withColumn("week_of_year", lit(None).cast("int"))
    .withColumn("is_month_end", lit(False))
)

# Union default row
dim_date = dim_date.unionByName(default_row)

# Write to Gold
(dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_date"))

print("gold.dim_date built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 2 — Build dim_product
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load silver source
mara = spark.table("hackathon.silver.mara")

# Generate surrogate key safely (no global sort)
dim_product = (
    mara
    .withColumn("product_key", monotonically_increasing_id() + 1)
    .withColumn("effective_from_date", current_date())
    .withColumn("effective_to_date", lit(None).cast("date"))
    .withColumn("is_current_flag", lit(True))
)

# Select required columns explicitly (important for schema control)
dim_product = dim_product.select(
    "product_key",
    "matnr",
    "mtart",
    "matkl",
    "meins",
    "ersda",
    "effective_from_date",
    "effective_to_date",
    "is_current_flag"
)

# Create default UNKNOWN row using existing schema (safe way)
default_product = (
    dim_product.limit(0)
    .withColumn("product_key", lit(-1))
    .withColumn("matnr", lit("UNKNOWN"))
    .withColumn("mtart", lit(None).cast("string"))
    .withColumn("matkl", lit(None).cast("string"))
    .withColumn("meins", lit(None).cast("string"))
    .withColumn("ersda", lit(None).cast("date"))
    .withColumn("effective_from_date", lit(None).cast("date"))
    .withColumn("effective_to_date", lit(None).cast("date"))
    .withColumn("is_current_flag", lit(True))
)

# Union default row
dim_product = dim_product.unionByName(default_product)

# Write to Gold
(dim_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_product"))

print("gold.dim_product built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 3 — Build dim_location

# COMMAND ----------

from pyspark.sql.functions import *

# Load plant data from Silver
marc = spark.table("hackathon.silver.marc")

# Extract distinct plants
plant_df = (
    marc
    .select(trim(col("werks")).alias("plant_code"))
    .filter(col("plant_code").isNotNull())
    .dropDuplicates()
)

# Generate surrogate key safely (no window function)
dim_location = (
    plant_df
    .withColumn("location_key", monotonically_increasing_id() + 1)
)

# Reorder columns
dim_location = dim_location.select(
    "location_key",
    "plant_code"
)

# Create default UNKNOWN row safely
default_location = (
    dim_location.limit(0)
    .withColumn("location_key", lit(-1))
    .withColumn("plant_code", lit("UNKNOWN"))
)

# Union default row
dim_location = dim_location.unionByName(default_location)

# Write to Gold
(dim_location.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_location"))

print("gold.dim_location built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 4 — Build dim_storage

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver tables
mard = spark.table("hackathon.silver.mard")
dim_location = spark.table("hackathon.gold.dim_location")

# Extract distinct plant + storage
storage_df = (
    mard
    .select(
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code")
    )
    .filter(col("plant_code").isNotNull())
    .filter(col("storage_location_code").isNotNull())
    .dropDuplicates()
)

# Join to get location_key
storage_joined = (
    storage_df
    .join(dim_location, "plant_code", "left")
)

# Generate surrogate key safely
dim_storage = (
    storage_joined
    .withColumn("storage_key", monotonically_increasing_id() + 1)
    .select(
        "storage_key",
        "location_key",
        "plant_code",
        "storage_location_code"
    )
)

# Create default UNKNOWN row safely
default_storage = (
    dim_storage.limit(0)
    .withColumn("storage_key", lit(-1))
    .withColumn("location_key", lit(-1))
    .withColumn("plant_code", lit("UNKNOWN"))
    .withColumn("storage_location_code", lit("UNKNOWN"))
)

# Union default row
dim_storage = dim_storage.unionByName(default_storage)

# Write to Gold
(dim_storage.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_storage"))

print("gold.dim_storage built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 5 — Build dim_supplier

# COMMAND ----------

from pyspark.sql.functions import *

# Load supplier IDs from PO header
ekko = spark.table("hackathon.silver.ekko")

supplier_df = (
    ekko
    .select(lpad(trim(col("lifnr")), 10, "0").alias("supplier_id"))
    .filter(col("supplier_id").isNotNull())
    .dropDuplicates()
)

# Generate surrogate key safely
dim_supplier = (
    supplier_df
    .withColumn("supplier_key", monotonically_increasing_id() + 1)
)

# Add default UNKNOWN row
default_supplier = (
    dim_supplier.limit(0)
    .withColumn("supplier_key", lit(-1))
    .withColumn("supplier_id", lit("UNKNOWN"))
)

dim_supplier = dim_supplier.unionByName(default_supplier)

# Write Gold table
(dim_supplier.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_supplier"))

print("gold.dim_supplier built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##🟢 STEP 6 — Build dim_currency

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver sources
ekko = spark.table("hackathon.silver.ekko")

# Extract distinct currency codes
currency_df = (
    ekko
    .select(trim(col("waers")).alias("currency_code"))
    .filter(col("currency_code").isNotNull())
    .dropDuplicates()
)

# Generate surrogate key safely
dim_currency = (
    currency_df
    .withColumn("currency_key", monotonically_increasing_id() + 1)
    .select("currency_key", "currency_code")
)

# Create default UNKNOWN row safely
default_currency = (
    dim_currency.limit(0)
    .withColumn("currency_key", lit(-1))
    .withColumn("currency_code", lit("UNKNOWN"))
)

# Union default row
dim_currency = dim_currency.unionByName(default_currency)

# Write to Gold
(dim_currency.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_currency"))

print("gold.dim_currency built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 7 — Build dim_uom

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver sources
mara = spark.table("hackathon.silver.mara")
ekpo = spark.table("hackathon.silver.ekpo")

# Extract UOM from MARA
uom_mara = (
    mara
    .select(trim(col("meins")).alias("uom_code"))
    .filter(col("uom_code").isNotNull())
)

# Extract UOM from EKPO (if column exists)
uom_ekpo = (
    ekpo
    .select(trim(col("meins")).alias("uom_code"))
    .filter(col("uom_code").isNotNull())
)

# Combine and deduplicate
uom_df = (
    uom_mara
    .unionByName(uom_ekpo)
    .dropDuplicates()
)

# Generate surrogate key safely
dim_uom = (
    uom_df
    .withColumn("uom_key", monotonically_increasing_id() + 1)
    .select("uom_key", "uom_code")
)

# Create default UNKNOWN row safely
default_uom = (
    dim_uom.limit(0)
    .withColumn("uom_key", lit(-1))
    .withColumn("uom_code", lit("UNKNOWN"))
)

# Union default row
dim_uom = dim_uom.unionByName(default_uom)

# Write to Gold
(dim_uom.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_uom"))

print("gold.dim_uom built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 8 — Build dim_batch

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver sources
mchb = spark.table("hackathon.silver.mchb")

# Extract distinct material + batch
batch_df = (
    mchb
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("charg")).alias("batch_code")
    )
    .filter(col("batch_code").isNotNull())
    .dropDuplicates()
)

# Generate surrogate key safely
dim_batch = (
    batch_df
    .withColumn("batch_key", monotonically_increasing_id() + 1)
    .select("batch_key", "matnr", "batch_code")
)

# Create default NO_BATCH row safely
default_batch = (
    dim_batch.limit(0)
    .withColumn("batch_key", lit(-1))
    .withColumn("matnr", lit("UNKNOWN"))
    .withColumn("batch_code", lit("NO_BATCH"))
)

# Union default row
dim_batch = dim_batch.unionByName(default_batch)

# Write to Gold
(dim_batch.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_batch"))

print("gold.dim_batch built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 9 — Build dim_customer

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver sources
demand_actual = spark.table("hackathon.silver.demand_actual")

# Extract distinct customers
customer_df = (
    demand_actual
    .select(trim(col("market")).alias("customer_id"))
    .filter(col("customer_id").isNotNull())
    .dropDuplicates()
)

# Generate surrogate key safely
dim_customer = (
    customer_df
    .withColumn("customer_key", monotonically_increasing_id() + 1)
    .select("customer_key", "customer_id")
)

# Create default UNKNOWN row safely
default_customer = (
    dim_customer.limit(0)
    .withColumn("customer_key", lit(-1))
    .withColumn("customer_id", lit("UNKNOWN"))
)

# Union default row
dim_customer = dim_customer.unionByName(default_customer)

# Write to Gold
(dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_customer"))

print("gold.dim_customer built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##🟢 STEP 10 — Build dim_customer_product

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver source
demand_actual = spark.table("hackathon.silver.demand_actual")

# Load Gold dimensions
dim_customer = spark.table("hackathon.gold.dim_customer")
dim_product = spark.table("hackathon.gold.dim_product")

# Extract distinct customer + product combinations
cust_prod_df = (
    demand_actual
    .select(
        trim(col("market")).alias("customer_id"),
        lpad(trim(col("material")), 18, "0").alias("matnr")
    )
    .filter(col("customer_id").isNotNull())
    .filter(col("matnr").isNotNull())
    .dropDuplicates()
)

# Join with surrogate keys
cust_prod_joined = (
    cust_prod_df
    .join(dim_customer, "customer_id", "left")
    .join(dim_product, "matnr", "left")
)

# Generate surrogate key safely
dim_customer_product = (
    cust_prod_joined
    .withColumn("customer_product_key", monotonically_increasing_id() + 1)
    .select(
        "customer_product_key",
        "customer_key",
        "product_key"
    )
)

# Create default -1 row safely
default_cp = (
    dim_customer_product.limit(0)
    .withColumn("customer_product_key", lit(-1))
    .withColumn("customer_key", lit(-1))
    .withColumn("product_key", lit(-1))
)

# Union default row
dim_customer_product = dim_customer_product.unionByName(default_cp)

# Write to Gold
(dim_customer_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_customer_product"))

print("gold.dim_customer_product built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##🟢 STEP 11 — Build dim_location_product

# COMMAND ----------

from pyspark.sql.functions import *

# Load Silver source
demand_actual = spark.table("hackathon.silver.demand_actual")

# Load Gold dimensions
dim_customer = spark.table("hackathon.gold.dim_customer")
dim_product = spark.table("hackathon.gold.dim_product")

# Extract distinct customer + product combinations
cust_prod_df = (
    demand_actual
    .select(
        trim(col("market")).alias("customer_id"),
        lpad(trim(col("material")), 18, "0").alias("matnr")
    )
    .filter(col("customer_id").isNotNull())
    .filter(col("matnr").isNotNull())
    .dropDuplicates()
)

# Join with surrogate keys
cust_prod_joined = (
    cust_prod_df
    .join(dim_customer, "customer_id", "left")
    .join(dim_product, "matnr", "left")
)

# Generate surrogate key safely
dim_customer_product = (
    cust_prod_joined
    .withColumn("customer_product_key", monotonically_increasing_id() + 1)
    .select(
        "customer_product_key",
        "customer_key",
        "product_key"
    )
)

# Create default -1 row safely
default_cp = (
    dim_customer_product.limit(0)
    .withColumn("customer_product_key", lit(-1))
    .withColumn("customer_key", lit(-1))
    .withColumn("product_key", lit(-1))
)

# Union default row
dim_customer_product = dim_customer_product.unionByName(default_cp)

# Write to Gold
(dim_customer_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.dim_customer_product"))

print("gold.dim_customer_product built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 Fact Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 12 — Build fact_inventory

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Sources
# ---------------------------
mard = spark.table("hackathon.silver.mard")
mchb = spark.table("hackathon.silver.mchb")
mbew = spark.table("hackathon.silver.mbew")

# ---------------------------
# Load Gold Dimensions (select only required columns)
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_location = spark.table("hackathon.gold.dim_location") \
    .select(
        col("plant_code"),
        col("location_key").alias("dim_location_key")
    )

dim_storage = spark.table("hackathon.gold.dim_storage") \
    .select(
        "plant_code",
        "storage_location_code",
        "storage_key"
    )

dim_batch = spark.table("hackathon.gold.dim_batch") \
    .select(
        "matnr",
        "batch_code",
        "batch_key"
    )

# ---------------------------
# 1️⃣ Prepare Non-Batch Stock (MARD)
# ---------------------------
mard_prepared = (
    mard
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code"),
        lit("NO_BATCH").alias("batch_code"),
        col("labst").alias("unrestricted_qty"),
        col("insme").alias("quality_qty"),
        col("speme").alias("blocked_qty")
    )
)

# ---------------------------
# 2️⃣ Prepare Batch Stock (MCHB)
# ---------------------------
mchb_prepared = (
    mchb
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code"),
        trim(col("charg")).alias("batch_code"),
        col("clabs").alias("unrestricted_qty"),
        col("cinsm").alias("quality_qty"),
        col("cspem").alias("blocked_qty")
    )
)

# ---------------------------
# 3️⃣ Combine Inventory
# ---------------------------
inventory_union = mard_prepared.unionByName(mchb_prepared)

# ---------------------------
# 4️⃣ Join Valuation
# ---------------------------
inventory_with_value = (
    inventory_union
    .join(
        mbew.select(
            lpad(trim(col("matnr")), 18, "0").alias("matnr"),
            col("effective_unit_cost")
        ),
        "matnr",
        "left"
    )
)

# ---------------------------
# 5️⃣ Join Surrogate Keys (NO AMBIGUITY)
# ---------------------------
fact_inventory = (
    inventory_with_value
    .join(dim_product, "matnr", "left")
    .join(dim_location, "plant_code", "left")
    .join(dim_storage,
          ["plant_code", "storage_location_code"],
          "left")
    .join(dim_batch,
          ["matnr", "batch_code"],
          "left")
)

# ---------------------------
# 6️⃣ Add Date Key (Current Snapshot)
# ---------------------------
fact_inventory = fact_inventory.withColumn(
    "date_key",
    date_format(current_date(), "yyyyMMdd").cast("int")
)

# ---------------------------
# 7️⃣ Calculate Measures
# ---------------------------
fact_inventory = (
    fact_inventory
    .withColumn("unrestricted_qty", coalesce(col("unrestricted_qty"), lit(0)))
    .withColumn("quality_qty", coalesce(col("quality_qty"), lit(0)))
    .withColumn("blocked_qty", coalesce(col("blocked_qty"), lit(0)))
    .withColumn("available_stock_qty",
                col("unrestricted_qty") + col("quality_qty"))
    .withColumn("total_stock_qty",
                col("unrestricted_qty") +
                col("quality_qty") +
                col("blocked_qty"))
    .withColumn("total_stock_value",
                col("total_stock_qty") *
                coalesce(col("effective_unit_cost"), lit(0)))
)

# ---------------------------
# 8️⃣ Final Select (Explicit Keys)
# ---------------------------
fact_inventory_final = fact_inventory.select(
    "date_key",
    "product_key",
    col("dim_location_key").alias("location_key"),
    "storage_key",
    "batch_key",
    "unrestricted_qty",
    "quality_qty",
    "blocked_qty",
    "available_stock_qty",
    "total_stock_qty",
    "total_stock_value"
)

# ---------------------------
# 9️⃣ Write Gold Table
# ---------------------------
(fact_inventory_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_inventory"))

print("gold.fact_inventory built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 13 — Build fact_purchase_order

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Sources
# ---------------------------
ekpo = spark.table("hackathon.silver.ekpo")
ekko = spark.table("hackathon.silver.ekko")
eket = spark.table("hackathon.silver.eket_agg")
ekbe = spark.table("hackathon.silver.ekbe_agg")

# ---------------------------
# Load Gold Dimensions (ONLY required columns)
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_supplier = spark.table("hackathon.gold.dim_supplier") \
    .select("supplier_id", "supplier_key")

dim_location = spark.table("hackathon.gold.dim_location") \
    .select(
        col("plant_code"),
        col("location_key").alias("dim_location_key")
    )

dim_storage = spark.table("hackathon.gold.dim_storage") \
    .select(
        "plant_code",
        "storage_location_code",
        "storage_key"
    )

dim_currency = spark.table("hackathon.gold.dim_currency") \
    .select(
        col("currency_code"),
        col("currency_key")
    )

dim_uom = spark.table("hackathon.gold.dim_uom") \
    .select(
        col("uom_code"),
        col("uom_key")
    )

# ---------------------------
# 1️⃣ Join Header + Item
# ---------------------------
po_base = (
    ekpo
    .join(ekko, "ebeln", "left")
)

# ---------------------------
# 2️⃣ Join Aggregations
# ---------------------------
po_enriched = (
    po_base
    .join(eket, ["ebeln", "ebelp"], "left")
    .join(ekbe, ["ebeln", "ebelp"], "left")
)

# ---------------------------
# 3️⃣ Standardize Columns
# ---------------------------
po_clean = (
    po_enriched
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        lpad(trim(col("lifnr")), 10, "0").alias("lifnr"),
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code"),
        trim(col("waers")).alias("currency_code"),
        trim(col("meins")).alias("uom_code"),
        col("ekko.aedat").alias("po_date"),
        col("menge").alias("order_quantity"),
        coalesce(col("delivered_qty"), lit(0)).alias("delivered_quantity"),
        col("netpr").alias("net_price")
    )
)

# ---------------------------
# 4️⃣ Join Surrogate Keys (NO AMBIGUITY)
# ---------------------------
fact_po = (
    po_clean
    .join(dim_product, "matnr", "left")
    .join(dim_supplier,
      po_clean.lifnr == dim_supplier.supplier_id,
      "left")
    .join(dim_location, "plant_code", "left")
    .join(dim_storage,
          ["plant_code", "storage_location_code"],
          "left")
    .join(dim_currency, "currency_code", "left")
    .join(dim_uom, "uom_code", "left")
)

# ---------------------------
# 5️⃣ Add Date Key
# ---------------------------
fact_po = fact_po.withColumn(
    "date_key",
    date_format(col("po_date"), "yyyyMMdd").cast("int")
)

# ---------------------------
# 6️⃣ Calculate Measures
# ---------------------------
fact_po = (
    fact_po
    .withColumn("order_quantity", coalesce(col("order_quantity"), lit(0)))
    .withColumn("delivered_quantity", coalesce(col("delivered_quantity"), lit(0)))
    .withColumn("open_quantity",
                col("order_quantity") - col("delivered_quantity"))
    .withColumn("net_order_value",
                col("order_quantity") * coalesce(col("net_price"), lit(0)))
)

# ---------------------------
# 7️⃣ Final Select (Explicit Keys)
# ---------------------------
fact_po_final = fact_po.select(
    "date_key",
    "product_key",
    "supplier_key",
    col("dim_location_key").alias("location_key"),
    "storage_key",
    "currency_key",
    "uom_key",
    "order_quantity",
    "delivered_quantity",
    "open_quantity",
    "net_order_value"
)

# ---------------------------
# 8️⃣ Write Gold Table
# ---------------------------
(fact_po_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_purchase_order"))

print("gold.fact_purchase_order built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 14 — Build fact_demand_actual

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Source
# ---------------------------
demand_actual = spark.table("hackathon.silver.demand_actual")

# ---------------------------
# Load Gold Dimensions (ONLY required columns)
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_customer = spark.table("hackathon.gold.dim_customer") \
    .select("customer_id", "customer_key")

# ---------------------------
# 1️⃣ Standardize Business Keys
# ---------------------------
fact_base = (
    demand_actual
    .select(
        lpad(trim(col("material")), 18, "0").alias("matnr"),
        trim(col("market")).alias("customer_id"),
        col("period_date"),
        col("actual_qty").alias("actual_quantity"),
        col("net_actual_qty").alias("net_actual_quantity"),
        col("actual_revenue")
    )
)

# ---------------------------
# 2️⃣ Join Surrogate Keys
# ---------------------------
fact_joined = (
    fact_base
    .join(dim_product, "matnr", "left")
    .join(dim_customer, "customer_id", "left")
)

# ---------------------------
# 3️⃣ Add Date Key
# ---------------------------
fact_joined = fact_joined.withColumn(
    "date_key",
    date_format(col("period_date"), "yyyyMMdd").cast("int")
)

# ---------------------------
# 4️⃣ Handle Null Measures Safely
# ---------------------------
fact_final = (
    fact_joined
    .withColumn("actual_quantity", coalesce(col("actual_quantity"), lit(0)))
    .withColumn("net_actual_quantity", coalesce(col("net_actual_quantity"), lit(0)))
    .withColumn("actual_revenue", coalesce(col("actual_revenue").cast("float"), lit(0)))
    .select(
        "date_key",
        "product_key",
        "customer_key",
        "actual_quantity",
        "net_actual_quantity",
        "actual_revenue"
    )
)

# ---------------------------
# 5️⃣ Write Gold Table
# ---------------------------
(fact_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_demand_actual"))

print("gold.fact_demand_actual built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 15 — Build fact_demand_forecast

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Source
# ---------------------------
demand_forecast = spark.table("hackathon.silver.demand_forecast")

# ---------------------------
# Load Gold Dimensions (ONLY required columns)
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_customer = spark.table("hackathon.gold.dim_customer") \
    .select("customer_id", "customer_key")

# ---------------------------
# 1️⃣ Standardize Business Keys
# ---------------------------
forecast_base = (
    demand_forecast
    .select(
        lpad(trim(col("material")), 18, "0").alias("matnr"),
        trim(col("market")).alias("customer_id"),
        col("period_date"),
        col("consensus_demand"),
        col("ibp_forecast"),
        col("budget_volumes")
    )
)

# ---------------------------
# 2️⃣ Join Surrogate Keys
# ---------------------------
forecast_joined = (
    forecast_base
    .join(dim_product, "matnr", "left")
    .join(dim_customer, "customer_id", "left")
)

# ---------------------------
# 3️⃣ Add Date Key
# ---------------------------
forecast_joined = forecast_joined.withColumn(
    "date_key",
    date_format(col("period_date"), "yyyyMMdd").cast("int")
)

# ---------------------------
# 4️⃣ Cast Measures to DECIMAL (VERY IMPORTANT)
# ---------------------------
forecast_final = (
    forecast_joined
    .withColumn("forecast_consensus",
        coalesce(col("consensus_demand").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .withColumn("forecast_ibp",
        coalesce(col("ibp_forecast").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .withColumn("forecast_budget",
        coalesce(col("budget_volumes").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .select(
        "date_key",
        "product_key",
        "customer_key",
        "forecast_consensus",
        "forecast_ibp",
        "forecast_budget"
    )
)

# ---------------------------
# 5️⃣ Drop Table If Exists (Avoid Schema Conflict)
# ---------------------------
spark.sql("DROP TABLE IF EXISTS hackathon.gold.fact_demand_forcast")

# ---------------------------
# 6️⃣ Write Gold Table
# ---------------------------
(forecast_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_demand_forcast"))

print("gold.fact_demand_forecast built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 16 — Build fact_batch_release_extern

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Sources
# ---------------------------
qals = spark.table("hackathon.silver.qals")
qave = spark.table("hackathon.silver.qave_agg")

# ---------------------------
# Load Gold Dimensions
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_location = spark.table("hackathon.gold.dim_location") \
    .select("plant_code", "location_key")

dim_batch = spark.table("hackathon.gold.dim_batch") \
    .select("matnr", "batch_code", "batch_key")

# ---------------------------
# 1️⃣ Join QALS + QAVE
# ---------------------------
quality_base = qals.join(qave, "prueflos", "left")

# ---------------------------
# 2️⃣ Standardize Business Keys
# ---------------------------
quality_clean = (
    quality_base
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werk")).alias("plant_code"),
        trim(col("charg")).alias("batch_code"),
        col("ersteldat").alias("inspection_date"),
        col("losmenge").cast("decimal(18,4)").alias("inspection_lot_qty"),
        col("lmengeist").cast("decimal(18,4)").alias("inspected_qty")
    )
)

# ---------------------------
# 3️⃣ Join Surrogate Keys
# ---------------------------
fact_quality = (
    quality_clean
    .join(dim_product, "matnr", "left")
    .join(dim_location, "plant_code", "left")
    .join(dim_batch,
          (quality_clean.matnr == dim_batch.matnr) &
          (quality_clean.batch_code == dim_batch.batch_code),
          "left")
)

# ---------------------------
# 4️⃣ Add Date Key
# ---------------------------
fact_quality = fact_quality.withColumn(
    "date_key",
    date_format(col("inspection_date"), "yyyyMMdd").cast("int")
)

# ---------------------------
# 5️⃣ Safe Measure Handling
# ---------------------------
fact_quality = (
    fact_quality
    .withColumn("inspection_lot_qty",
                coalesce(col("inspection_lot_qty"), lit(0).cast("decimal(18,4)")))
    .withColumn("inspected_qty",
                coalesce(col("inspected_qty"), lit(0).cast("decimal(18,4)")))
    .withColumn("defective_qty",
                col("inspection_lot_qty") - col("inspected_qty"))
)

# ---------------------------
# 6️⃣ Final Select
# ---------------------------
fact_quality_final = fact_quality.select(
    "date_key",
    "product_key",
    "location_key",
    "batch_key",
    "inspection_lot_qty",
    "inspected_qty",
    "defective_qty"
)

# ---------------------------
# 7️⃣ Write Gold Table
# ---------------------------
spark.sql("DROP TABLE IF EXISTS hackathon.gold.fact_batch_release_extern")

(fact_quality_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_batch_release_extern"))

print("gold.fact_batch_release_extern built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 17 — Build fact_inventory_month_end_stock

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Sources
# ---------------------------
mardh = spark.table("hackathon.silver.mardh")
mchbh = spark.table("hackathon.silver.mchbh")
mbewh = spark.table("hackathon.silver.mbewh")

# ---------------------------
# Load Gold Dimensions (ONLY required columns)
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_location = spark.table("hackathon.gold.dim_location") \
    .select("plant_code", "location_key")

dim_storage = spark.table("hackathon.gold.dim_storage") \
    .select("plant_code", "storage_location_code", "storage_key")

dim_batch = spark.table("hackathon.gold.dim_batch") \
    .select("matnr", "batch_code", "batch_key")

# ---------------------------
# 1️⃣ Prepare Non-Batch Historical
# ---------------------------
mardh_prepared = (
    mardh
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code"),
        lit("NO_BATCH").alias("batch_code"),
        col("lfgja"),
        col("lfmon"),
        col("labst"),
        col("insme"),
        col("speme")
    )
)

# ---------------------------
# 2️⃣ Prepare Batch Historical
# ---------------------------
mchbh_prepared = (
    mchbh
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werks")).alias("plant_code"),
        trim(col("lgort")).alias("storage_location_code"),
        trim(col("charg")).alias("batch_code"),
        col("lfgja"),
        col("lfmon"),
        col("clabs").alias("labst"),
        col("cinsm").alias("insme"),
        col("cspem").alias("speme")
    )
)

# ---------------------------
# 3️⃣ Union Historical Stock
# ---------------------------
historical_stock = mardh_prepared.unionByName(mchbh_prepared)

# ---------------------------
# 4️⃣ Join Historical Valuation
# ---------------------------
historical_stock = (
    historical_stock
    .join(
        mbewh.select(
            lpad(trim(col("matnr")), 18, "0").alias("matnr"),
            col("lfgja"),
            col("lfmon"),
            col("verpr").cast("decimal(18,4)")
        ),
        ["matnr", "lfgja", "lfmon"],
        "left"
    )
)

# ---------------------------
# 5️⃣ Create Month-End Date
# ---------------------------
historical_stock = (
    historical_stock
    .withColumn(
        "period_date",
        last_day(
            to_date(
                concat(col("lfgja"),
                       lpad(col("lfmon"), 2, "0"),
                       lit("01")),
                "yyyyMMdd"
            )
        )
    )
    .withColumn(
        "date_key",
        date_format(col("period_date"), "yyyyMMdd").cast("int")
    )
)

# ---------------------------
# 6️⃣ Join Surrogate Keys
# ---------------------------
fact_mes = (
    historical_stock
    .join(dim_product, "matnr", "left")
    .join(dim_location, "plant_code", "left")
    .join(dim_storage,
          ["plant_code", "storage_location_code"],
          "left")
    .join(dim_batch,
          ["matnr", "batch_code"],
          "left")
)

# ---------------------------
# 7️⃣ Cast Measures Safely
# ---------------------------
fact_mes = (
    fact_mes
    .withColumn("unrestricted_qty",
        coalesce(col("labst").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .withColumn("quality_qty",
        coalesce(col("insme").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .withColumn("blocked_qty",
        coalesce(col("speme").cast("decimal(18,4)"),
                 lit(0).cast("decimal(18,4)")))
    .withColumn("total_stock_qty",
        col("unrestricted_qty") +
        col("quality_qty") +
        col("blocked_qty"))
    .withColumn("total_stock_value",
        col("total_stock_qty") *
        coalesce(col("verpr"), lit(0).cast("decimal(18,4)")))
)

# ---------------------------
# 8️⃣ Final Select
# ---------------------------
fact_mes_final = fact_mes.select(
    "date_key",
    "product_key",
    "location_key",
    "storage_key",
    "batch_key",
    "unrestricted_qty",
    "quality_qty",
    "blocked_qty",
    "total_stock_qty",
    "total_stock_value"
)

# ---------------------------
# 9️⃣ Drop & Write Table
# ---------------------------
spark.sql("DROP TABLE IF EXISTS hackathon.gold.fact_inventory_month_end_stock")

(fact_mes_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_inventory_month_end_stock"))

print("gold.fact_inventory_month_end_stock built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 18 — Build fact_batch_release_internal

# COMMAND ----------

from pyspark.sql.functions import *

# ---------------------------
# Load Silver Source
# ---------------------------
qals = spark.table("hackathon.silver.qals")

# ---------------------------
# Load Gold Dimensions
# ---------------------------
dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_location = spark.table("hackathon.gold.dim_location") \
    .select("plant_code", "location_key")

dim_batch = spark.table("hackathon.gold.dim_batch") \
    .select("matnr", "batch_code", "batch_key")

# ---------------------------
# 1️⃣ Filter Internal Inspections
# Adjust filter condition if needed
# Example: inspection type not equal to vendor type
# ---------------------------

internal_qals = (
    qals
    .filter(col("art") != "01")  # adjust based on your SAP inspection type logic
)

# ---------------------------
# 2️⃣ Standardize Business Keys
# ---------------------------

quality_clean = (
    internal_qals
    .select(
        lpad(trim(col("matnr")), 18, "0").alias("matnr"),
        trim(col("werk")).alias("plant_code"),
        trim(col("charg")).alias("batch_code"),
        col("ersteldat").alias("inspection_date"),
        col("losmenge").cast("decimal(18,4)").alias("inspection_lot_qty"),
        col("lmengeist").cast("decimal(18,4)").alias("inspected_qty")
    )
)

# ---------------------------
# 3️⃣ Join Surrogate Keys
# ---------------------------

fact_internal = (
    quality_clean
    .join(dim_product, "matnr", "left")
    .join(dim_location, "plant_code", "left")
    .join(dim_batch,
          (quality_clean.matnr == dim_batch.matnr) &
          (quality_clean.batch_code == dim_batch.batch_code),
          "left")
)

# ---------------------------
# 4️⃣ Add Date Key
# ---------------------------

fact_internal = fact_internal.withColumn(
    "date_key",
    date_format(col("inspection_date"), "yyyyMMdd").cast("int")
)

# ---------------------------
# 5️⃣ Calculate Measures
# ---------------------------

fact_internal = (
    fact_internal
    .withColumn("inspection_lot_qty",
                coalesce(col("inspection_lot_qty"), lit(0).cast("decimal(18,4)")))
    .withColumn("inspected_qty",
                coalesce(col("inspected_qty"), lit(0).cast("decimal(18,4)")))
    .withColumn("defective_qty",
                col("inspection_lot_qty") - col("inspected_qty"))
)

# ---------------------------
# 6️⃣ Final Select
# ---------------------------

fact_internal_final = fact_internal.select(
    "date_key",
    "product_key",
    "location_key",
    "batch_key",
    "inspection_lot_qty",
    "inspected_qty",
    "defective_qty"
)

# ---------------------------
# 7️⃣ Write Gold Table
# ---------------------------

spark.sql("DROP TABLE IF EXISTS hackathon.gold.fact_batch_release_internal")

(fact_internal_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date_key")
    .option("overwriteSchema", "true")
    .saveAsTable("hackathon.gold.fact_batch_release_internal"))

print("gold.fact_batch_release_internal built successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ## 🟢 STEP 18 — Build Gold Snapshot Fact Table

# COMMAND ----------

from pyspark.sql.functions import *

snapshot = spark.table("hackathon.silver.demand_forecast_snapshot")

dim_product = spark.table("hackathon.gold.dim_product") \
    .select("matnr", "product_key")

dim_customer = spark.table("hackathon.gold.dim_customer") \
    .select("customer_id", "customer_key")

gold_snapshot = (
    snapshot
    .withColumn("date_key", date_format(col("target_date"), "yyyyMMdd").cast("int"))
    .withColumn("snapshot_key", date_format(col("snapshot_date"), "yyyyMMdd").cast("int"))
    .join(dim_product, snapshot.material == dim_product.matnr, "left")
    .join(dim_customer, snapshot.market == dim_customer.customer_id, "left")
    .select(
        "date_key",              # target month
        "snapshot_key",          # forecast creation month
        "product_key",
        "customer_key",
        "forecast_consensus"
    )
)

gold_snapshot.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date_key") \
    .saveAsTable("hackathon.gold.fact_demand_forecast_snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 GOLD LAYER – FULL VALIDATION & INTEGRITY CHECK

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 1 — Check All Gold Tables Exist

# COMMAND ----------

spark.sql("SHOW TABLES IN hackathon.gold").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP-2 — Check All Gold Tables Exist

# COMMAND ----------

from pyspark.sql.functions import col

print("fact_demand_actual NULL keys:",
      spark.table("hackathon.gold.fact_demand_actual")
      .filter(col("product_key").isNull() | col("customer_key").isNull())
      .count())

print("fact_purchase_order NULL keys:",
      spark.table("hackathon.gold.fact_purchase_order")
      .filter(col("product_key").isNull() |
              col("supplier_key").isNull() |
              col("location_key").isNull())
      .count())

print("fact_batch_release_extern NULL keys:",
      spark.table("hackathon.gold.fact_batch_release_extern")
      .filter(col("product_key").isNull() |
              col("location_key").isNull() |
              col("batch_key").isNull())
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 3 — Check NULL Surrogate Keys (All Facts)

# COMMAND ----------

from pyspark.sql.functions import count

print("fact_demand_actual duplicates:",
      spark.table("hackathon.gold.fact_demand_actual")
      .groupBy("date_key", "product_key", "customer_key")
      .count()
      .filter(col("count") > 1)
      .count())

print("fact_batch_release_extern duplicates:",
      spark.table("hackathon.gold.fact_batch_release_extern")
      .groupBy("date_key", "product_key", "location_key", "batch_key")
      .count()
      .filter(col("count") > 1)
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 4 — Negative Measure Check

# COMMAND ----------

print("Negative demand quantity:",
      spark.table("hackathon.gold.fact_demand_actual")
      .filter(col("actual_quantity") < 0)
      .count())

print("Negative inventory quantity:",
      spark.table("hackathon.gold.fact_inventory")
      .filter(col("total_stock_qty") < 0)
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 5 — Date Key Integrity Check

# COMMAND ----------

print("Orphan date_keys in demand:",
      spark.table("hackathon.gold.fact_demand_actual")
      .join(spark.table("hackathon.gold.dim_date"),
            "date_key",
            "left_anti")
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 6 — Partition Validation

# COMMAND ----------

spark.sql("DESCRIBE DETAIL hackathon.gold.fact_demand_actual") \
    .select("partitionColumns") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 7 — Schema Validation

# COMMAND ----------

spark.table("hackathon.gold.fact_demand_actual").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 STEP 8 — Sanity Aggregates

# COMMAND ----------

from pyspark.sql.functions import sum

spark.table("hackathon.gold.fact_demand_actual") \
    .agg(sum("actual_quantity").alias("total_qty"),
         sum("actual_revenue").alias("total_revenue")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Optimization of Gold layer Galaxy schema 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 1 — Enable Delta Auto Optimize

# COMMAND ----------

# Enable automatic file compaction and optimized writes
# Improves small file handling and write performance

spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

print("Auto optimize enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 2 — Optimize fact_demand_actual

# COMMAND ----------

# Optimize physically compacts small files
# ZORDER clusters data by frequently filtered columns (product, customer)

spark.sql("""
OPTIMIZE hackathon.gold.fact_demand_actual
ZORDER BY (product_key, customer_key)
""")

print("fact_demand_actual optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 3 — Optimize fact_demand_forecast

# COMMAND ----------

# Similar clustering for forecast table
# Helps Power BI when slicing by product and customer

spark.sql("""
OPTIMIZE hackathon.gold.fact_demand_forcast
ZORDER BY (product_key, customer_key)
""")

print("fact_demand_forcast optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 4 — Optimize fact_purchase_order
# MAGIC

# COMMAND ----------

# Procurement analysis usually filters by product, supplier, plant
# ZORDER improves data skipping and join performance

spark.sql("""
OPTIMIZE hackathon.gold.fact_purchase_order
ZORDER BY (product_key, supplier_key, location_key)
""")

print("fact_purchase_order optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 5 — Optimize fact_batch_release_extern

# COMMAND ----------

# Quality reporting filters by product, plant, batch
# ZORDER clusters data for faster defect analysis

spark.sql("""
OPTIMIZE hackathon.gold.fact_batch_release_extern
ZORDER BY (product_key, location_key, batch_key)
""")

print("fact_batch_release_extern optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 6 — Optimize fact_inventory (if available)

# COMMAND ----------

# Inventory analytics heavily filter by product and location
# ZORDER improves stock reporting speed

spark.sql("""
OPTIMIZE hackathon.gold.fact_inventory
ZORDER BY (product_key, location_key)
""")

print("fact_inventory optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 7 — Compute Table Statistics

# COMMAND ----------

# Computes table statistics for Spark query planner
# Improves join strategy and execution planning

spark.sql("ANALYZE TABLE hackathon.gold.fact_demand_actual COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE hackathon.gold.fact_purchase_order COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE hackathon.gold.fact_batch_release_extern COMPUTE STATISTICS")

print("Statistics computed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 8 — Vacuum (Cleanup Old Files)

# COMMAND ----------

# Removes old Delta files after retention period
# 168 hours = 7 days (safe default for time travel)

spark.sql("VACUUM hackathon.gold.fact_demand_actual RETAIN 168 HOURS")

print("Vacuum completed")