import pandas as pd
import numpy as np
import yaml
import logging
import logging.config
from pathlib import Path

# Setup logging
logging.config.fileConfig("config/logging.conf")
logger = logging.getLogger("dataIngestion")


class SilverProcessor:
    def __init__(self, config_path="config/settings.yaml"):
        logger.info("Initializing Silver Processor")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.bronze_path = Path(self.config["paths"]["bronze_layer"])
        self.silver_path = Path(self.config["paths"]["silver_layer"])
        self.silver_path.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Load Bronze Data
    # -----------------------------
    def load_bronze(self):
        logger.info("Loading Bronze layer data")

        self.orders = pd.read_parquet(self.bronze_path / "orders.parquet")
        self.customers = pd.read_parquet(self.bronze_path / "customers.parquet")
        self.order_items = pd.read_parquet(self.bronze_path / "order_items.parquet")
        self.payments = pd.read_parquet(self.bronze_path / "payments.parquet")
        self.products = pd.read_parquet(self.bronze_path / "products.parquet")
        self.reviews = pd.read_parquet(self.bronze_path / "reviews.parquet")

    # -----------------------------
    # Clean & Join
    # -----------------------------
    def build_order_level(self):
        logger.info("Building order-level dataset")

        # Keep only delivered orders
        orders = self.orders[self.orders["order_status"] == "delivered"].copy()

        # Convert timestamps
        date_cols = [
            "order_purchase_timestamp",
            "order_delivered_customer_date"
        ]
        for col in date_cols:
            orders[col] = pd.to_datetime(orders[col])

        # Join customers
        df = orders.merge(
            self.customers,
            on="customer_id",
            how="left"
        )

        # Aggregate order items
        items_agg = self.order_items.groupby("order_id").agg(
            items_count=("order_item_id", "count"),
            total_price=("price", "sum"),
            total_freight=("freight_value", "sum"),
            avg_item_price=("price", "mean")
        ).reset_index()

        df = df.merge(items_agg, on="order_id", how="left")

        # Payments
        payments_agg = self.payments.groupby("order_id").agg(
            payment_value=("payment_value", "sum"),
            payment_type=("payment_type", lambda x: x.mode()[0])
        ).reset_index()

        df = df.merge(payments_agg, on="order_id", how="left")

        # Reviews
        reviews_agg = self.reviews.groupby("order_id").agg(
            review_score=("review_score", "mean")
        ).reset_index()

        df = df.merge(reviews_agg, on="order_id", how="left")

        logger.info(f"Order-level rows: {len(df):,}")
        return df

    # -----------------------------
    # Feature Engineering
    # -----------------------------
    def add_features(self, df):
        logger.info("Adding time & business features")

        # Time features
        df["year"] = df["order_purchase_timestamp"].dt.year
        df["month"] = df["order_purchase_timestamp"].dt.month
        df["day"] = df["order_purchase_timestamp"].dt.day
        df["day_of_week"] = df["order_purchase_timestamp"].dt.dayofweek
        df["hour"] = df["order_purchase_timestamp"].dt.hour
        df["quarter"] = df["order_purchase_timestamp"].dt.quarter
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

        # Delivery time
        df["delivery_days"] = (
            df["order_delivered_customer_date"]
            - df["order_purchase_timestamp"]
        ).dt.days

        # Order value features
        df["order_value"] = df["total_price"] + df["total_freight"]
        df["freight_ratio"] = df["total_freight"] / df["order_value"]

        # Order value categories
        df["order_value_category"] = pd.qcut(
            df["order_value"],
            q=4,
            labels=["Low", "Medium", "High", "Premium"]
        )

        # Review categories
        df["review_category"] = pd.cut(
            df["review_score"],
            bins=[0, 2, 3, 4, 5],
            labels=["Poor", "Fair", "Good", "Excellent"]
        )

        return df

    # -----------------------------
    # RFM Calculation
    # -----------------------------
    def build_rfm(self, df):
        logger.info("Calculating RFM features")

        snapshot_date = df["order_purchase_timestamp"].max() + pd.Timedelta(days=1)

        rfm = df.groupby("customer_unique_id").agg(
            recency=("order_purchase_timestamp",
                     lambda x: (snapshot_date - x.max()).days),
            frequency=("order_id", "nunique"),
            monetary=("order_value", "sum")
        ).reset_index()

        # RFM scores (1–5)
        rfm["R"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
        rfm["F"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
        rfm["M"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

        rfm["RFM_SCORE"] = (
            rfm["R"].astype(int) +
            rfm["F"].astype(int) +
            rfm["M"].astype(int)
        )

        return rfm

    # -----------------------------
    # Customer Summary
    # -----------------------------
    def build_customer_summary(self, df):
        logger.info("Building customer summary table")

        customer = df.groupby("customer_unique_id").agg(
            total_orders=("order_id", "nunique"),
            total_spend=("order_value", "sum"),
            avg_order_value=("order_value", "mean"),
            first_order=("order_purchase_timestamp", "min"),
            last_order=("order_purchase_timestamp", "max")
        ).reset_index()

        customer["days_active"] = (
            customer["last_order"] - customer["first_order"]
        ).dt.days

        customer["orders_per_month"] = (
            customer["total_orders"] /
            (customer["days_active"] / 30).replace(0, 1)
        )

        return customer

    # -----------------------------
    # Run Pipeline
    # -----------------------------
    def run(self):
        self.load_bronze()
        orders = self.build_order_level()
        orders = self.add_features(orders)

        rfm = self.build_rfm(orders)
        print(rfm)
        customers = self.build_customer_summary(orders)
        print(customers)
        # Save
        orders.to_parquet(self.silver_path / "orders_silver.parquet", index=False)
        rfm.to_parquet(self.silver_path / "rfm.parquet", index=False)
        customers.to_parquet(self.silver_path / "customers_silver.parquet", index=False)

        logger.info("Silver layer successfully created")


def main():
    processor = SilverProcessor()
    processor.run()


if __name__ == "__main__":
    main()
