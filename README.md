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

**Customer 360 Data Integration – GCP Bootcamp Project 3**
Date: July 9, 2025
---
**Objective**
To build a unified Customer 360 view by integrating and transforming raw customer data across online transactions, in-store purchases, loyalty programs, and customer service interactions using GCP tools.

---
**Project Structure**
 
Customer360/
├── gs://bronze-customer360/        # Raw data
├── gs://curated-silver/            # Cleaned CSVs via PySpark (Colab)
├── BigQuery:
│   ├── customer360_silver          # Cleaned tables
│   └── customer360_gold            # Summary analytics tables
└── Looker Studio Dashboard         # Visual insights

---
 **Steps Executed**
 ---
 **Step 1: Upload Raw CSVs**
All 9 raw datasets were uploaded to:
gs://bronze-customer360/

---
**Step 2: Data Cleaning (in Google Colab)**
The Cloud Shell Editor was used to submit a PySpark job to Dataproc by running a shell command that created the cluster and submitted the cleaning script customer360_cleaning.py. This script processed the raw datasets stored in the bronze GCS bucket. Each dataset was cleaned by renaming columns, dropping nulls and duplicates, and formatting timestamp fields. The cleaned output was written back to GCS under gs://curated-silver/{TableName}_cleaned/ folders, such as Customers_cleaned, Products_cleaned, and so on.

Applied .strip() and .replace(" ", "_") to column names

Dropped nulls and duplicates

Saved cleaned CSVs to:
gs://curated-silver/{TableName}_cleaned/

---
**Step 3: Load Cleaned Data to BigQuery**
Created customer360_silver dataset. Loaded each file with:

bq load \
--source_format=CSV \
--skip_leading_rows=1 \
--replace=true \
--autodetect \
sakina-gcp:customer360_silver.Customers \
gs://curated-silver/Customers_cleaned/part-*.csv
(Similarly repeated for: Products, Stores, LoyaltyAccounts, LoyaltyTransactions, Agents, InStoreTransactions, OnlineTransactions, and CustomerServiceInteractions.)

---
 **Step 4: Create Summary Tables (Gold Layer)**
Dataset: customer360_gold
Created these tables:
---
**1. Average Order Value**

bq query --use_legacy_sql=false
"CREATE OR REPLACE TABLE customer360_gold.avg_order_value_summary AS
SELECT
  CustomerID,
  COUNT(TransactionID) AS TotalOrders,
  SUM(TransactionAmount) AS TotalSpent,
  ROUND(SUM(TransactionAmount)/COUNT(TransactionID), 2) AS AvgOrderValue
FROM customer360_silver.OnlineTransactions
WHERE TransactionAmount IS NOT NULL
GROUP BY CustomerID;"


---
**2. Loyalty Tier Summary**
 
bq query --use_legacy_sql=false 
"CREATE OR REPLACE TABLE customer360_gold.loyalty_points_summary AS
SELECT
  CustomerID,
  PointsEarned,
  TierLevel,
  CASE
    WHEN PointsEarned >= 4000 THEN 'Loyalty Champion'
    WHEN PointsEarned >= 2500 THEN 'High-Value Customer'
    ELSE 'Standard'
  END AS CustomerSegment
FROM customer360_silver.LoyaltyAccounts;"


---
**3. InStore vs Online Transactions**
 
bq query --use_legacy_sql=false 
"CREATE OR REPLACE TABLE customer360_gold.instore_vs_online_summary AS
SELECT DATE(DateTime) AS TransactionDate, 'InStore' AS TransactionType, COUNT(*) AS TotalTransactions
FROM customer360_silver.InStoreTransactions
GROUP BY TransactionDate

UNION ALL

SELECT DATE(DateTime), 'Online', COUNT(*)
FROM customer360_silver.OnlineTransactions
GROUP BY DATE(DateTime);"


---
**4. Agent Resolution Summary**
 
 bq query --use_legacy_sql=false 
 "CREATE OR REPLACE TABLE customer360_gold.agent_resolution_summary AS
SELECT
  AgentID,
  COUNT(InteractionID) AS TotalInteractions,
  SUM(CASE WHEN ResolutionStatus = 'Resolved' THEN 1 ELSE 0 END) AS ResolvedCount,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN ResolutionStatus = 'Resolved' THEN 1 ELSE 0 END), COUNT(InteractionID)), 2) AS ResolutionRate
FROM customer360_silver.CustomerServiceInteractions
GROUP BY AgentID;"

---
**Step 5: Build Looker Studio Dashboard**
Connected all 4 gold tables
---
**Created:**

 Bar chart: Avg Order Value

 Pie chart: Loyalty Tier

 Line chart: InStore vs Online

 Agent Resolution Rate (Bar)
 
 --- 

**Final Results**

Cleaned and loaded 9 datasets into BigQuery

Built 4 gold summary tables

Created Customer 360 dashboard in Looker Studio

Avoided manual uploads with bq automation

---
  
 
 
 
