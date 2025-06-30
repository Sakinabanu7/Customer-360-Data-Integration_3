## README.md – Customer 360 Data Integration_3

---

### Project Title:

**Customer 360 Data Integration Pipeline using GCP**

---

### Project Overview:

This project builds a 360-degree customer view by integrating data from multiple sources, including:

* Online transactions
* In-store purchases
* Customer service interactions
* Loyalty program data

The cleaned and unified view enables data-driven insights using GCP tools like **Cloud Storage**, **Dataproc**, **BigQuery**, and **Power BI/Looker**.

---

### Tools and Technologies Used:

| Tool                       | Purpose                           |
| -------------------------- | --------------------------------- |
| Google Cloud Storage (GCS) | Bronze and Silver data layer      |
| Apache Spark on Dataproc   | Cleaning        |
| BigQuery                   | Silver and Gold data warehouse    |
| Cloud Composer (Airflow)   | Pipeline orchestration            |
| Power BI / Looker Studio   | Dashboards for insights           |
| Python (PySpark)           | ETL scripting                     |
| GitHub                     | Code and documentation versioning |

---

## Step-by-Step Execution Plan

---

### Step 1: Raw Data Preparation (Bronze Layer)

* All raw `.csv` files (9 total) are organized under:
  `gs://customer360-raw-data/raw/`
* Datasets include:

  * `OnlineTransactions.csv`
  * `InStoreTransactions.csv`
  * `CustomerServiceInteractions.csv`
  * `LoyaltyAccounts.csv`
  * `Customers.csv`, etc.

---

### Step 2: Silver Schema in BigQuery

* Dataset: `customer360_silver`
* DDLs created for:

  * `Customers`, `Products`, `Agents`, etc.
* Tables define correct types, keys, formats
* BigQuery staging tables ready to receive cleaned Parquet data

---

### Step 3: Dataproc Cluster Setup

* Created a **single-node Dataproc cluster**
* Used **PySpark** for cleaning and type conversion
* Cluster region matches GCS and BigQuery for efficiency

---

### Step 4: Data Cleaning on Dataproc (Silver Layer)

* PySpark script for each dataset:

  * Drops nulls, formats DateTime, casts data types
  * Saves output as `.parquet` to:
     `gs://customer360-silver-data/[TableName]/`

**Sample Task:**
For `OnlineTransactions.csv`:

```python
df_clean = df.dropna(subset=["OrderID", "Amount"]) \
             .withColumn("DateTime", to_timestamp(col("DateTime")))
```

* Scripts stored in `gs://customer360-scripts/`

---

### Step 5: Load Cleaned Data to BigQuery (Silver Layer)

* Use `bq load` or Composer DAG to load `.parquet` files:

```bash
bq load --source_format=PARQUET \
customer360_silver.OnlineTransactions \
gs://customer360-silver-data/OnlineTransactions/
```

---

### Step 6: Gold Layer Aggregations in BigQuery

* Dataset: `customer360_gold`
* Four output tables:

  1. **AOV** – SUM(Amount)/COUNT(OrderID) per Product/Category/Location
  2. **Customer Segments** – Spend, frequency, tier
  3. **Peak Times** – Analyze DateTime usage online vs. in-store
  4. **Agent KPIs** – Resolution rates per agent

---

### Step 7: Cloud Composer DAG

* DAG handles:

  * Ingestion validation
  * Dataproc job execution
  * BigQuery load
  * Success notification
* DAG name: `customer360_ingestion_dag`

---

### Step 8: Dashboard Creation

* Connected **Power BI / Looker Studio** to BigQuery
* Dashboard shows:

  * Customer segments & loyalty tiers
  * AOV trends by location
  * Peak day/hour visualizations
  * Agent performance summaries

---

## Project Deliverables

| Deliverable             | Location                            |
| ----------------------- | ----------------------------------- |
| Raw Files               | `gs://customer360-raw-data/`        |
| Cleaned Files           | `gs://customer360-silver-data/`     |
| BigQuery Silver Dataset | `customer360_silver`                |
| BigQuery Gold Dataset   | `customer360_gold`                  |
| DAG Script              | `customer360_ingestion_dag.py`      |
| PySpark Scripts         | `clean_[table].py`                  |
| Dashboard               | Power BI report file or Looker link |
| GitHub Repo             | Scripts, DAG JSON, README.md, SQLs  |

---

## Interview-Possible Q\&A

---

### Q1: **If you run your PySpark script twice, how do you prevent duplicate entries in BigQuery?**

**Answer:**
To prevent duplication in BigQuery:

* Use `MERGE` statement instead of INSERT:

```sql
MERGE INTO customer360_gold.OnlineTransactions T
USING customer360_silver.OnlineTransactions S
ON T.OrderID = S.OrderID
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

* Load into a **staging table first**, then deduplicate
* Use `ROW_NUMBER()` or `DISTINCT` in queries
* Track load status via `BatchID` or `LoadAudit` tables
* Design Composer DAGs with idempotent task retries

---

 
 
