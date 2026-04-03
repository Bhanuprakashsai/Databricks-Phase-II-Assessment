# Databricks Phase-II Assessment

## Overview

This project demonstrates the design and implementation of an end-to-end production-grade data engineering platform for SmartRetail Corp using modern lakehouse architecture on Databricks.

## Business Context

SmartRetail Corp processes thousands of daily transactions across multiple cities with the following challenges:
* High-volume data ingestion (daily incremental loads)
* Performance issues with large joins
* Lack of proper partitioning and optimization
* No standardized deployment pipelines
* Business users need near real-time insights

## Solution Architecture

Built a scalable, optimized lakehouse system supporting:
* Incremental ingestion with metadata tracking
* Efficient transformations using Delta Lake
* Optimized analytics queries with star schema modeling
* Comprehensive Spark and Delta Lake optimizations
* Production-ready data quality and performance

---

## Implementation Summary

### 🧻 Data Volume
* **Catalog**: QA_Assessment
* **Source**: `/Volumes/QA_Assessment/Bronze/raw`
* **Total Records**: 355,000 across 4 datasets
  * customers.csv: 50,000 records
  * products.csv: 5,000 records
  * orders.csv: 100,000 records
  * order_items.csv: 200,000 records

---

## 🏛️ Medallion Architecture Implementation

### 🥉 Bronze Layer - Raw Data Ingestion

**Notebook**: `src/Bronze`

**Strategy**:
* Append-only storage preserving raw data
* CSV ingestion with schema inference
* Metadata tracking for lineage

**Tables Created**:
* `QA_Assessment.Bronze.bronze_customers` - 50,000 records
* `QA_Assessment.Bronze.bronze_products` - 5,000 records
* `QA_Assessment.Bronze.bronze_orders` - 100,000 records
* `QA_Assessment.Bronze.bronze_order_items` - 200,000 records

**Metadata Columns Added**:
* `ingestion_timestamp` - Timestamp of data ingestion
* `source_file_name` - Source file path for lineage tracking

**Key Features**:
* Reusable ingestion function for all datasets
* Schema validation and sampling
* Error handling and logging

---

### 🥈 Silver Layer - Cleaned & Enriched Data

**Notebook**: `src/Silver`

**Strategy**:
* Incremental processing using `created_at` and `updated_at` timestamps
* Deduplication using window functions and primary keys
* UPSERT operations via Delta Lake MERGE
* Data enrichment through joins

**Tables Created**:
* `QA_Assessment.Silver.silver_customers` - 50,000 records
* `QA_Assessment.Silver.silver_products` - 5,000 records
* `QA_Assessment.Silver.silver_orders` - 100,000 records
* `QA_Assessment.Silver.silver_order_items` - 200,000 records
* `QA_Assessment.Silver.silver_orders_enriched` - 100,000 records (with joins)

**Incremental Logic**:
```python
# Load only new or updated records
df_incremental = df_source.filter(
    (col("created_at") > last_run_timestamp) | 
    (col("updated_at") > last_run_timestamp)
)
```

**UPSERT Pattern**:
```python
delta_table.alias("target").merge(
    source.alias("source"),
    merge_condition
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Enrichments**:
* Orders joined with customer details (name, city, state)
* Orders joined with product category information
* Sample product names for quick reference

---

### 🥇 Gold Layer - Analytics Ready

**Notebook**: `src/Gold`

**Strategy**:
* Star schema modeling for optimal BI performance
* Denormalized facts for fast queries
* Pre-aggregated metrics for dashboards
* Partitioning by date for query performance

#### Dimension Tables

**dim_customers** - 50,000 records
* Customer demographic information
* Signup year segmentation
* SCD Type 1 implementation

**dim_products** - 5,000 records
* Product catalog with categories
* Price segmentation (Budget, Mid-Range, Premium, Luxury)
* List price tracking

#### Fact Tables

**fact_sales** - 200,000 records
* Granular transaction-level data
* **Partitioned by**: `order_year`, `order_month`
* Foreign keys to all dimensions
* Multiple date dimensions (year, month, day, month_key)
* Measures: quantity, unit_price, line_total, order_total
* Denormalized dimensions for performance

#### Aggregate Tables (Pre-computed Metrics)

**agg_revenue_by_state**
* Total revenue, orders, customers per state
* Average, max, min order values
* Filtered to delivered orders only

**agg_top_products**
* Products ranked by revenue
* Total quantity sold and order counts
* Average price per product
* Revenue ranking for quick Top-N queries

**agg_sales_daily**
* Daily sales trends
* Daily revenue, orders, customers
* Average order value per day

**agg_sales_monthly**
* Monthly sales trends
* Month-over-month growth calculations
* Unique customer counts
* Revenue growth percentage

---

## ⚡ Performance Optimizations

### 🚀 Spark Optimizations

**Notebook**: `src/Optimizations`

**1. Broadcast Joins**
* Applied to small dimension tables (products: 5K records)
* Avoids expensive shuffle operations
* **Expected Improvement**: 2-5x faster joins

**2. Caching Strategy**
* Demonstrated for DataFrames accessed multiple times
* Proper cache lifecycle management
* **Expected Improvement**: 3-10x faster repeated queries
* **Note**: Not available on serverless, but demonstrated best practices

**3. Repartition vs Coalesce**
* Coalesce for reducing partitions (no shuffle)
* Repartition for increasing parallelism or even distribution
* Repartition by column for co-location
* **Best Practice**: ~128MB per partition

**4. Skewed Data Handling**
* Analyzed data skew patterns by state
* Leveraged Adaptive Query Execution (AQE)
* AQE automatically handles skewed joins at runtime
* Skew detection and partition splitting

### 📦 Delta Lake Optimizations

**1. OPTIMIZE (File Compaction)**
* Compacted small files into larger ones
* Reduced file count and read overhead
* Applied to `fact_sales` table
* **Expected Improvement**: 30-50% faster reads

**2. Z-ORDERING**
* Applied on `state` and `customer_id` columns
* Co-locates related data for efficient data skipping
* Dramatically reduces data scanned for filtered queries
* **Expected Improvement**: 2-10x faster filtered queries

```sql
OPTIMIZE QA_Assessment.Gold.fact_sales
ZORDER BY (state, customer_id)
```

**3. VACUUM**
* Demonstrated dry run for old file cleanup
* Balances time travel capabilities vs storage costs
* Retention: 168 hours (7 days)
* **Expected Improvement**: 20-40% storage reduction

**4. Time Travel**
* Query historical versions by version number or timestamp
* Enables auditing and rollback capabilities
* Critical for data governance and compliance

```sql
SELECT * FROM table VERSION AS OF 2
SELECT * FROM table TIMESTAMP AS OF '2024-01-01'
```

**5. Schema Enforcement & Evolution**
* Schema validation prevents incompatible data
* Schema evolution with `mergeSchema` option
* Documented best practices for schema changes

---

## 📋 Project Structure

```
Capsotone_Project_Phase_II/
├── src/
│   ├── Bronze.py          # Bronze layer ingestion
│   ├── Silver.py          # Silver layer incremental processing
│   ├── Gold.py            # Gold layer analytics modeling
│   └── Optimizations.py   # Spark & Delta Lake optimizations
├── .github/
│   └── workflows/         # CI/CD pipelines (to be implemented)
└── README.md              # This file
```

---

## 📈 Performance Metrics

### Expected Improvements

| Optimization | Improvement | Impact |
|---|---|---|
| Broadcast Joins | 2-5x faster | Large fact × small dimension joins |
| Caching | 3-10x faster | Repeated DataFrame access |
| Z-ORDERING | 2-10x faster | Filtered queries on indexed columns |
| OPTIMIZE | 30-50% faster | Read operations after compaction |
| VACUUM | 20-40% reduction | Storage costs |

---

## 🛠️ Technologies & Tools

* **Platform**: Databricks Lakehouse (AWS)
* **Compute**: Serverless
* **Storage**: Delta Lake
* **Processing**: Apache Spark (PySpark)
* **Query Engine**: Photon
* **Catalog**: Unity Catalog
* **Orchestration**: Databricks Workflows
* **Version Control**: Git/GitHub

---

## ✅ Completed Deliverables

* ✅ Bronze, Silver, Gold tables (Medallion Architecture)
* ✅ Incremental pipeline implementation with UPSERT logic
* ✅ Optimized Spark jobs with broadcast joins and AQE
* ✅ Delta optimization scripts (OPTIMIZE, Z-ORDER, VACUUM)
* ✅ Star schema with fact and dimension tables
* ✅ Pre-aggregated metrics for BI
* ✅ Comprehensive optimization documentation

---

## 🚀 Next Steps

### 1. CI/CD Pipeline
* Connect Databricks with GitHub
* Create GitHub Actions workflows
* Implement automated testing
* Environment promotion (Dev → QA → Prod)

### 2. AI-Powered BI
* Create Lakeview dashboards:
  * Revenue trends over time
  * Top-performing products and categories
  * Customer segmentation analysis
  * Geographic performance heatmaps
* Enable Databricks Genie for natural language queries
* Example queries:
  * "Show revenue for last month"
  * "Top 5 products by sales"
  * "Customer growth by state"

### 3. Operational Excellence
* Schedule regular OPTIMIZE and VACUUM jobs
* Set up data quality monitoring
* Implement alerting for pipeline failures
* Create data lineage documentation
* Monitor query performance metrics

---

## 📚 Documentation

### Key Concepts Demonstrated

**Medallion Architecture**
* Bronze: Raw data preservation
* Silver: Cleaned and conformed data
* Gold: Business-ready aggregates

**Incremental Processing**
* Timestamp-based filtering
* Change Data Capture (CDC) pattern
* MERGE operations for UPSERT

**Performance Optimization**
* Broadcast joins for dimension tables
* Partitioning strategies
* Z-Ordering for data skipping
* File compaction

**Data Quality**
* Schema enforcement
* Deduplication
* Metadata tracking
* Time travel for auditing

---

## 🤝 Contributing

This project demonstrates production-grade data engineering best practices including:
* Clean code with reusable functions
* Comprehensive error handling
* Performance optimization
* Documentation and comments
* Modular notebook design

---

## 📝 License

This is an assessment project for educational purposes.

---

## 💬 Contact

For questions about this implementation, please refer to the notebook documentation and inline comments.