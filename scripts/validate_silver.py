import pandas as pd
from pathlib import Path

silver = Path("data/silver")

orders = pd.read_parquet(silver / "orders_silver.parquet")
customers = pd.read_parquet(silver / "customers_silver.parquet")
rfm = pd.read_parquet(silver / "rfm.parquet")

print("Orders:", orders.shape)
print("Customers:", customers.shape)
print("RFM:", rfm.shape)

assert orders["order_value"].min() >= 0
assert rfm["RFM_SCORE"].between(3, 15).all()

print("Silver layer validation passed ✅")
