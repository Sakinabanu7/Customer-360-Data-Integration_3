from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Start Spark session
spark = SparkSession.builder.appName("Customer360Cleaning").getOrCreate()

# GCS Paths
input_bucket = "gs://bronze-customer360"
output_bucket = "gs://curated-silver"

# Utility function to clean column names
def clean_column_names(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.strip().replace(" ", "_"))
    return df

# ========== 1. Customers ==========
df_customers = spark.read.option("header", True).csv(f"{input_bucket}/Customers.csv")
df_customers = clean_column_names(df_customers).dropDuplicates().dropna(subset=["CustomerID"])
df_customers.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/Customers")

# ========== 2. Products ==========
df_products = spark.read.option("header", True).csv(f"{input_bucket}/Products.csv")
df_products = clean_column_names(df_products).dropDuplicates().dropna(subset=["ProductID"])
df_products.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/Products")

# ========== 3. Online Transactions ==========
df_online = spark.read.option("header", True).csv(f"{input_bucket}/OnlineTransactions.csv")
df_online = clean_column_names(df_online).withColumn("DateTime", to_timestamp("DateTime")) \
            .dropDuplicates().dropna(subset=["OrderID"])
df_online.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/OnlineTransactions")

# ========== 4. Stores ==========
df_stores = spark.read.option("header", True).csv(f"{input_bucket}/Stores.csv")
df_stores = clean_column_names(df_stores).dropDuplicates().dropna(subset=["StoreID"])
df_stores.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/Stores")

# ========== 5. In-Store Transactions ==========
df_instore = spark.read.option("header", True).csv(f"{input_bucket}/InStoreTransactions.csv")
df_instore = clean_column_names(df_instore).withColumn("DateTime", to_timestamp("DateTime")) \
             .dropDuplicates().dropna(subset=["TransactionID"])
df_instore.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/InStoreTransactions")

# ========== 6. Agents ==========
df_agents = spark.read.option("header", True).csv(f"{input_bucket}/Agents.csv")
df_agents = clean_column_names(df_agents).dropDuplicates().dropna(subset=["AgentID"])
df_agents.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/Agents")

# ========== 7. Customer Service Interactions ==========
df_interactions = spark.read.option("header", True).csv(f"{input_bucket}/CustomerServiceInteractions.csv")
df_interactions = clean_column_names(df_interactions).withColumn("DateTime", to_timestamp("DateTime")) \
                   .dropDuplicates().dropna(subset=["InteractionID"])
df_interactions.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/CustomerServiceInteractions")

# ========== 8. Loyalty Accounts ==========
df_loyalty = spark.read.option("header", True).csv(f"{input_bucket}/LoyaltyAccounts.csv")
df_loyalty = clean_column_names(df_loyalty).withColumn("JoinDate", to_timestamp("JoinDate")) \
             .dropDuplicates().dropna(subset=["LoyaltyID"])
df_loyalty.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/LoyaltyAccounts")

# ========== 9. Loyalty Transactions ==========
df_loyalty_txn = spark.read.option("header", True).csv(f"{input_bucket}/LoyaltyTransactions.csv")
df_loyalty_txn = clean_column_names(df_loyalty_txn).withColumn("DateTime", to_timestamp("DateTime")) \
                 .dropDuplicates().dropna(subset=["LoyaltyID", "DateTime"])
df_loyalty_txn.write.mode("overwrite").option("header", True).csv(f"{output_bucket}/LoyaltyTransactions")
