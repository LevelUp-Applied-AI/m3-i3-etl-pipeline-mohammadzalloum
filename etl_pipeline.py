"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers_df = pd.read_sql("SELECT * FROM customers", engine)
    products_df = pd.read_sql("SELECT * FROM products", engine)
    orders_df = pd.read_sql("SELECT * FROM orders", engine)
    order_items_df = pd.read_sql("SELECT * FROM order_items", engine)

    print(f"Extracted customers: {len(customers_df)} rows")
    print(f"Extracted products: {len(products_df)} rows")
    print(f"Extracted orders: {len(orders_df)} rows")
    print(f"Extracted order_items: {len(order_items_df)} rows")

    return {
        "customers": customers_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    # 1) Join orders with order_items
    merged_df = orders.merge(order_items, on="order_id", how="inner")

    # 2) Join with products
    merged_df = merged_df.merge(products, on="product_id", how="inner")

    # 3) Filter out cancelled orders
    merged_df = merged_df[merged_df["status"] != "cancelled"]

    # 4) Filter out suspicious quantities
    merged_df = merged_df[merged_df["quantity"] <= 100]

    # 5) Compute line_total
    merged_df["line_total"] = merged_df["quantity"] * merged_df["unit_price"]

    print(f"Rows after filtering and joins: {len(merged_df)}")

    # Aggregate customer metrics
    customer_summary = (
        merged_df.groupby("customer_id")
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum")
        )
        .reset_index()
    )

    customer_summary["avg_order_value"] = (
        customer_summary["total_revenue"] / customer_summary["total_orders"]
    )

    # Compute top_category per customer based on highest revenue
    category_revenue = (
        merged_df.groupby(["customer_id", "category"])["line_total"]
        .sum()
        .reset_index()
    )

    top_category = (
        category_revenue.sort_values(
            ["customer_id", "line_total"],
            ascending=[True, False]
        )
        .drop_duplicates(subset=["customer_id"])
        [["customer_id", "category"]]
        .rename(columns={"category": "top_category"})
    )

    # Bring customer info
    customers = customers.rename(columns={"name": "customer_name"})

    customer_summary = customer_summary.merge(
        customers[["customer_id", "customer_name", "city"]],
        on="customer_id",
        how="left"
    )

    customer_summary = customer_summary.merge(
        top_category,
        on="customer_id",
        how="left"
    )

    # Reorder columns
    customer_summary = customer_summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
        ]
    ]

    print(f"Transformed customer summary: {len(customer_summary)} rows")

    return customer_summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customer_id": not df["customer_id"].duplicated().any(),
        "total_orders_positive": (df["total_orders"] > 0).all(),
    }

    for check_name, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"{check_name}: {status}")

    critical_checks = [
        "no_null_customer_id",
        "no_null_customer_name",
        "total_revenue_positive",
        "no_duplicate_customer_id",
        "total_orders_positive",
    ]

    failed_checks = [check for check in critical_checks if not checks[check]]

    if failed_checks:
        raise ValueError(f"Validation failed for checks: {failed_checks}")

    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows to PostgreSQL table: customer_analytics")
    print(f"Saved CSV to: {csv_path}")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5433/amman_market"
    )

    engine = create_engine(database_url)

    print("Starting ETL pipeline...")

    data = extract(engine)
    customer_summary = transform(data)
    validation_results = validate(customer_summary)

    print("\nCustomer summary preview:")
    print(customer_summary.head())

    print("\nValidation results:")
    print(validation_results)

    load(customer_summary, engine, "output/customer_analytics.csv")

    print("\nETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
