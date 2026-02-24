# 🥉 Bronze Layer – Raw Data Ingestion Layer

## 📌 Overview

The Bronze Layer represents the **raw ingestion layer** of the Medallion Architecture.  
It is the first landing zone where source data from SAP and SAP IBP systems is stored in its original structure with minimal transformation.

This layer ensures:

- Raw data preservation
- Traceability
- Replay capability
- Auditability
- Schema transparency

---

## 🏗 Architecture Context

Medallion Flow:

Raw Source Systems  
→ Bronze (Raw Storage)  
→ Silver (Cleansed & Standardized)  
→ Gold (Business-Ready Analytical Model)

The Bronze layer acts as the **foundation of the entire analytics platform**.

---

## 📂 Data Sources Ingested

### SAP ERP Tables
- MARA (Material Master)
- MARC (Plant Data)
- MARD (Storage Location Stock)
- MCHB (Batch Stock)
- EKPO (Purchase Order Items)
- EKKO (Purchase Order Header)
- QALS (Inspection Lot Header)
- CKMLCR (Material Ledger)
- Others as required

### SAP IBP Tables
- Demand Actual
- Demand Forecast
- Budget Volumes
- Snapshot Forecast Data (if available)

---

## ⚙️ Ingestion Strategy

- Data stored in **ADLS (Azure Data Lake Storage)**
- CSV files loaded into Databricks
- Stored as **Delta Tables**
- Schema inferred at ingestion
- Original structure preserved
- Column names standardized to lowercase

No business logic applied in Bronze.

---

## 🧾 Transformations Applied in Bronze

Only minimal technical transformations:

- Header = True
- InferSchema = True
- Column name cleaning:
  - Lowercase
  - Replace spaces with "_"
  - Remove special characters
- Added ingestion timestamp column

Example:

```python
df.withColumn("ingestion_ts", current_timestamp())
```

---

## 🔐 Data Governance Principles

The Bronze layer:

- Is immutable (overwrite only during controlled re-ingestion)
- Stores raw source data
- Preserves original data fidelity
- Is NOT exposed to BI users
- Serves as audit & recovery layer

This ensures:

- Data lineage tracking
- Regulatory compliance
- Debugging capability

---

## 📊 Table Characteristics

| Attribute | Bronze Layer |
|------------|--------------|
| Data Quality | Raw |
| Business Rules | None |
| Deduplication | No |
| Surrogate Keys | No |
| Aggregations | No |
| Data Type Standardization | Minimal |
| Partitioning | Not business-based |

---

## 🚀 Why Bronze Layer is Important

1. Guarantees raw data preservation
2. Enables reprocessing if logic changes
3. Supports data lineage
4. Prevents contamination of business layer
5. Acts as single source of truth for ingestion

---

## 🧠 Design Philosophy

Bronze Layer follows:

- Schema-on-read approach
- Minimal transformation principle
- Append-only ingestion pattern
- Separation of concerns

It ensures engineering reliability before analytical modeling begins.

---

## 📈 Impact on Hackathon Solution

By implementing a structured Bronze layer:

- We ensured scalable ingestion
- Enabled structured Silver cleansing
- Built trusted Gold KPIs
- Maintained enterprise-grade architecture

This demonstrates production-level data engineering maturity.

---

# 🏁 Conclusion

The Bronze layer is the **raw, trusted, ingestion foundation** of the supply chain analytics platform.

It enables reliable transformation into Silver and ultimately drives accurate executive KPIs in the Gold layer.