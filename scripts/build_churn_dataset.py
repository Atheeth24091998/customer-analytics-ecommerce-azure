import pandas as pd
import numpy as np
from pathlib import Path

silver = Path("data/silver")

orders = pd.read_parquet(silver / "orders_silver.parquet")
customers = pd.read_parquet(silver / "customers_silver.parquet")
rfm = pd.read_parquet(silver / "rfm.parquet")

# --------------------------------------------------
# CHURN LABEL
# --------------------------------------------------
orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"]
)

dataset_end = orders["order_purchase_timestamp"].max()

last_order = (
    orders.groupby("customer_unique_id")["order_purchase_timestamp"]
    .max()
    .reset_index()
)

last_order["days_since_last_order"] = (
    dataset_end - last_order["order_purchase_timestamp"]
).dt.days

last_order["churn"] = (last_order["days_since_last_order"] > 90).astype(int)

# --------------------------------------------------
# CUSTOMER-LEVEL AGGREGATES (FROM orders_silver)
# --------------------------------------------------
cust_features = orders.groupby("customer_unique_id").agg(
    order_count=("order_id", "nunique"),
    total_spend=("order_value", "sum"),
    avg_order_value=("order_value", "mean"),
    std_order_value=("order_value", "std"),
    total_items=("items_count", "sum"),
    avg_items_per_order=("items_count", "mean"),
    avg_freight_ratio=("freight_ratio", "mean"),
    avg_delivery_days=("delivery_days", "mean"),
    avg_review_score=("review_score", "mean")
).reset_index()

cust_features["single_purchase_customer"] = (
    cust_features["order_count"] == 1
).astype(int)

cust_features.fillna(0, inplace=True)

# --------------------------------------------------
# FINAL DATASET
# --------------------------------------------------
df = (
    cust_features
    .merge(customers, on="customer_unique_id", how="left")
    .merge(rfm, on="customer_unique_id", how="left")
    .merge(last_order[["customer_unique_id", "churn"]], on="customer_unique_id")
)

df.to_parquet(
    silver / "customer_churn_features.parquet",
    index=False
)

print("✅ Churn dataset created")
