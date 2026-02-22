import pandas as pd
import os

# --- Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # transform folder
PROC = os.path.join(SCRIPT_DIR, "..", "processed")       # processed folder (one level up)
WAREHOUSE = os.path.join(SCRIPT_DIR, "..", "warehouse")  # warehouse folder (one level up)
os.makedirs(WAREHOUSE, exist_ok=True)

# ---------- Read processed CSVs ----------
customers = pd.read_csv(os.path.join(PROC, "customers_proc.csv"))
flavours = pd.read_csv(os.path.join(PROC, "flavours_proc.csv"))
ingredients = pd.read_csv(os.path.join(PROC, "ingredients_proc.csv"))
providers = pd.read_csv(os.path.join(PROC, "providers_proc.csv"))
raw_materials = pd.read_csv(os.path.join(PROC, "raw_materials_proc.csv"))
recipes = pd.read_csv(os.path.join(PROC, "recipes_proc.csv"))
sales = pd.read_csv(os.path.join(PROC, "sales_transaction_proc.csv"))

# ---------- DIMENSIONS ----------
# dim_customer
dim_customer = customers[['customer_id', 'name', 'location_city', 'location_country']]
dim_customer.to_csv(os.path.join(WAREHOUSE, "dim_customer.csv"), index=False)

# dim_provider
dim_provider = providers
dim_provider.to_csv(os.path.join(WAREHOUSE, "dim_provider.csv"), index=False)

# dim_ingredient
dim_ingredient = ingredients
dim_ingredient.to_csv(os.path.join(WAREHOUSE, "dim_ingredient.csv"), index=False)

# dim_raw_material
dim_raw_material = raw_materials
dim_raw_material.to_csv(os.path.join(WAREHOUSE, "dim_raw_material.csv"), index=False)

# dim_flavour → latest batch
dim_flavour = flavours.sort_values("batch_number", ascending=False).drop_duplicates("flavour_id")
dim_flavour.to_csv(os.path.join(WAREHOUSE, "dim_flavour.csv"), index=False)

# dim_recipe
dim_recipe = recipes[['recipe_id', 'heat_process', 'yield']]
dim_recipe.to_csv(os.path.join(WAREHOUSE, "dim_recipe.csv"), index=False)

# dim_date from sales
sales['transaction_date'] = pd.to_datetime(sales['transaction_date'], errors='coerce')
dim_date = sales[['transaction_date']].dropna().drop_duplicates()
dim_date['year'] = dim_date['transaction_date'].dt.year
dim_date['month'] = dim_date['transaction_date'].dt.month
dim_date['quarter'] = dim_date['transaction_date'].dt.quarter
dim_date.to_csv(os.path.join(WAREHOUSE, "dim_date.csv"), index=False)

# ---------- FACT TABLES ----------
# fact_recipe_composition
fact_recipe_composition = recipes
fact_recipe_composition.to_csv(os.path.join(WAREHOUSE, "fact_recipe_composition.csv"), index=False)

# fact_sales
fact_sales = sales
fact_sales.to_csv(os.path.join(WAREHOUSE, "fact_sales.csv"), index=False)

print("✔ Transformation complete. Dimensions & facts saved in 'warehouse/'")
