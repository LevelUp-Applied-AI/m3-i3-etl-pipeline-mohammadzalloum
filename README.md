[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This project builds a Python ETL pipeline for the fictional **Amman Digital Market** database. The pipeline extracts data from PostgreSQL, transforms raw order data into customer-level analytics using Pandas, validates the transformed output with data quality checks, and loads the final result into both a new PostgreSQL table and a CSV file. :contentReference[oaicite:1]{index=1}

The final output summarizes each customer’s purchasing behavior, including:
- total number of valid orders
- total revenue
- average order value
- top product category by revenue

The pipeline also excludes:
- cancelled orders
- suspicious order items where `quantity > 100` :contentReference[oaicite:2]{index=2}


## Setup

1. Start the PostgreSQL container.
   If port `5432` is available:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d amman_market -f schema.sql
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```


## Output

After running the pipeline, the output is saved in two places:
1.PostgreSQL table: 
 customer_analytics 
2.CSV file: 
 output/customer_analytics.csv 

The output contains these columns:
   customer_id 
   customer_name 
   city 
   total_orders 
   total_revenue 
   avg_order_value 
   top_category 


## Quality Checks

The pipeline validates the transformed customer summary using these checks:
   no nulls in customer_id
   no nulls in customer_name
   total_revenue > 0 for all customers
   no duplicate customer_id values
   total_orders > 0 for all customers

Each check is printed as PASS or FAIL. If any critical check fails, the pipeline raises a ValueError.
---


## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
