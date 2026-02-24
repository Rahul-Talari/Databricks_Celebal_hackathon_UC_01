# 🥈 Silver Layer – Cleansed & Standardized Data Layer

## 📌 Overview

The Silver Layer represents the **cleansed, standardized, and structured transformation layer** in the Medallion Architecture.

It transforms raw Bronze data into:

- Cleaned
- Typed
- Deduplicated
- Business-aligned
- Relationally structured

datasets that are ready to be used for analytical modeling in the Gold layer.

---

## 🏗 Architecture Position

Raw Source Systems  
→ Bronze (Raw Storage)  
→ **Silver (Cleansed & Structured)**  
→ Gold (Business KPIs & Star/Galaxy Schema)

The Silver layer is the **data engineering core** of the solution.

---

## 🎯 Objectives of Silver Layer

1. Standardize column names
2. Enforce correct data types
3. Remove duplicates
4. Clean nulls
5. Format business keys
6. Apply minimal business logic
7. Create relational SAP structure
8. Prepare data for surrogate key mapping

---

## 📂 Tables Built in Silver Layer

### 🔹 Master Data Tables

- mara (Material Master)
- marc (Plant Data)
- mard (Storage Location Stock)
- mchb (Batch Stock)
- lfa1 (Supplier Master)

---

### 🔹 Transactional Tables

- ekko (Purchase Order Header)
- ekpo (Purchase Order Item)
- eket_agg (PO Schedule Aggregation)
- ekbe_agg (PO History Aggregation)
- qals (Inspection Lot Header)
- ckmlcr (Material Ledger)

---

### 🔹 Demand Planning Tables

- demand_actual
- demand_forecast
- demand_forecast_snapshot (if snapshot supported)

---

## 🔧 Transformations Applied

### 1️⃣ Column Standardization

- Converted all column names to lowercase
- Replaced spaces with underscores
- Removed special characters

Example:

```python
c.strip().lower().replace(" ", "_")
```

---

### 2️⃣ Business Key Formatting

SAP keys standardized for consistency:

- MATNR padded to 18 characters
- LIFNR padded to 10 characters
- Trimmed plant, storage, batch fields

Example:

```python
lpad(trim(col("matnr")), 18, "0")
```

---

### 3️⃣ Data Type Enforcement

Explicit casting applied to:

- Quantities → decimal(18,4)
- Prices → decimal(18,4)
- Dates → to_date()
- Numeric flags → int

This prevents type mismatch in Gold layer.

---

### 4️⃣ Date Conversion

Converted SAP date formats:

- YYYYMMDD → Date
- YYYYMM → Month-based date
- Snapshot dates handled explicitly

Example:

```python
to_date(col("ersteldat"), "yyyyMMdd")
```

---

### 5️⃣ Deduplication

Removed duplicate rows using primary business keys.

Example:

```python
.dropDuplicates(["matnr", "werks"])
```

---

### 6️⃣ Basic Aggregations

Aggregated heavy transactional tables to reduce duplication:

- eket_agg (delivery schedule)
- ekbe_agg (PO history)

Improves Gold layer performance.

---

## 🔐 Data Governance Principles

The Silver layer:

- Contains no surrogate keys
- Contains no business KPIs
- Preserves business grain
- Ensures clean joins
- Is not directly exposed to BI

This ensures separation between:

Engineering Layer (Silver)  
and  
Business Layer (Gold)

---

## 📊 Grain Management

Each Silver table preserves original business grain:

| Table | Grain |
|--------|-------|
| mara | 1 row per material |
| marc | 1 row per material + plant |
| mard | 1 row per material + plant + storage |
| mchb | 1 row per material + plant + batch |
| ekpo | 1 row per PO item |
| qals | 1 row per inspection lot |
| demand_actual | 1 row per product + customer + month |
| demand_forecast | 1 row per product + customer + month |

---

## 🚀 Performance Considerations

- No heavy joins in Silver
- No surrogate key generation
- Data typed before Gold
- Cleaned to avoid Spark casting errors
- Reduced shuffle operations

Silver prepares data for efficient Gold modeling.

---

## 🧠 Why Silver Layer is Critical

Without Silver:

- Gold layer would fail due to dirty keys
- KPI calculations would break
- Data type mismatches would occur
- Duplicate records would inflate metrics
- Snapshot modeling would be impossible

Silver ensures:

✔ Reliable joins  
✔ Clean foreign keys  
✔ Accurate KPI calculations  
✔ Stable BI model  

---

## 📈 Impact on Forecast KPIs

Silver enables:

- FCA Consensus Forecast
- FCA IBP Forecast
- Budget Attainment
- Snapshot-based FCA Lag (if snapshot exists)

By ensuring:

- Consistent product & customer keys
- Clean forecast and actual volumes
- Correct monthly date alignment

---

## 🏁 Conclusion

The Silver layer acts as the **structured transformation engine** of the solution.

It bridges raw system data and executive-ready KPIs by:

- Standardizing SAP & IBP datasets
- Enforcing business data types
- Preserving transactional grain
- Preparing conformed dimensions for Gold layer
