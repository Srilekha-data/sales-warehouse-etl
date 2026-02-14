import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = Path(__file__).resolve().parents[1] / "sales_warehouse.db"

def extract():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    products = pd.read_csv(DATA_DIR / "products.csv")
    orders = pd.read_csv(DATA_DIR / "orders.csv")
    return customers, products, orders

def transform(customers, products, orders):
    # Clean
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])

    # Join (warehouse style)
    fact_orders = (
        orders.merge(customers, on="CustomerID", how="left")
              .merge(products, on="ProductID", how="left")
    )

    # Metrics
    fact_orders["Revenue"] = fact_orders["Quantity"] * fact_orders["UnitPrice"]

    # Summary table
    revenue_by_customer = (
        fact_orders.groupby(["CustomerID", "CustomerName"], as_index=False)["Revenue"]
        .sum()
        .sort_values("Revenue", ascending=False)
    )

    revenue_by_category = (
        fact_orders.groupby(["Category"], as_index=False)["Revenue"]
        .sum()
        .sort_values("Revenue", ascending=False)
    )

    return fact_orders, revenue_by_customer, revenue_by_category

def load(customers, products, fact_orders, revenue_by_customer, revenue_by_category):
    conn = sqlite3.connect(DB_PATH)
    customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    products.to_sql("dim_products", conn, if_exists="replace", index=False)
    fact_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)
    revenue_by_customer.to_sql("agg_revenue_by_customer", conn, if_exists="replace", index=False)
    revenue_by_category.to_sql("agg_revenue_by_category", conn, if_exists="replace", index=False)
    conn.close()

def main():
    customers, products, orders = extract()
    fact_orders, rev_customer, rev_category = transform(customers, products, orders)
    load(customers, products, fact_orders, rev_customer, rev_category)

    print("✅ ETL completed successfully!")
    print(f"✅ SQLite database created: {DB_PATH}")
    print("\nTop Customers (Revenue):")
    print(rev_customer.head())

if __name__ == "__main__":
    main()
