# DataCase-Flavours
Project Overview

This project demonstrates an end-to-end ETL pipeline that transforms raw CSV data into a dimensional warehouse, ready for analytics and dashboards.

Key Highlights:

Ingest multiple raw datasets

Perform data quality checks (nulls, duplicates, timestamps)

Build dimensional model (Dimensions & Facts)

Generate warehouse-ready CSV outputs

Source Files
File Name	Description
customers.csv	Customer master data
flavours.csv	Flavours, with multiple batches
ingredients.csv	Ingredient details
providers.csv	Providers information
raw_materials.csv	Raw materials used in recipes
recipes.csv	Recipes & composition details
sales_transactions.csv	Sales transactions
ETL Layers
Layer	Purpose	Output Location
Raw	Original CSV files	raw/
Processed	Cleaned files, metadata added, audit logs	processed/
Warehouse	Dimension & Fact tables for analytics	warehouse/
Dimensions
Dimension Table	Key Columns / Notes
dim_customer	customer_id, name, city, country
dim_provider	provider_id, name, country
dim_ingredient	ingredient_id, name
dim_raw_material	raw_material_id, name
dim_flavour	flavour_id, latest batch, description
dim_recipe	recipe_id, heat_process, yield
dim_date	transaction_date, year, month, quarter
Fact Tables
Fact Table	Description
fact_recipe_composition	Recipe ingredient composition
fact_sales	Sales transactions
Audit Logs
Audit Log Column	Description
file	Name of ingested file
rows	Number of rows
duplicates	Duplicate rows count
nulls	Number of nulls
load_ts	Timestamp of ingestion
Notes

Nulls are handled during ingestion.

Latest batch used for dimension tables where applicable.

Outputs are CSVs ready for analytics and dashboards.

Referential integrity maintained between dimensions & facts.

Summary:
The repository demonstrates a complete pipeline:

Raw → Processed → Warehouse (Dimensions & Facts) → Analytics-ready CSVs

This structure ensures data quality, traceability, and easy use for dashboards.
