# Sales Warehouse ETL Pipeline

## Project Overview
This project demonstrates an end-to-end ETL pipeline that loads CSV data into a simple data warehouse style SQLite database.

## Data Sources
- customers.csv
- products.csv
- orders.csv

## ETL Steps
1. **Extract**: Reads CSV files from `/data`
2. **Transform**:
   - Joins customers + orders + products
   - Calculates revenue
   - Creates summary aggregates
3. **Load**: Writes dimension + fact + aggregate tables to SQLite

## Output
A SQLite database is created:
- `sales_warehouse.db`

Tables created:
- `dim_customers`
- `dim_products`
- `fact_orders`
- `agg_revenue_by_customer`
- `agg_revenue_by_category`

## How to Run
```bash
pip install pandas
python src/etl_pipeline.py
