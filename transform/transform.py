import pandas as pd
import os

# ------------------- Paths -------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROC = os.path.join(SCRIPT_DIR, "..", "processed")  # processed files from ingestion
WAREHOUSE = os.path.join(SCRIPT_DIR, "..", "warehouse")  # warehouse outside transform folder
os.makedirs(WAREHOUSE, exist_ok=True)  # create warehouse folder if it doesn't exist

# ------------------- Helper -------------------
def safe_read_csv(file_name):
    """Read a CSV if it exists, otherwise return empty DataFrame."""
    path = os.path.join(PROC, file_name)
    if os.path.exists(path):
        df = pd.read_csv(path)
        return df
    else:
        print(f"Warning: {file_name} not found in processed folder.")
        return pd.DataFrame()

# ------------------- Load Processed Files -------------------
customers = safe_read_csv("customers_proc.csv")
flavours = safe_read_csv("flavours_proc.csv")
ingredients = safe_read_csv("ingredients_proc.csv")
providers = safe_read_csv("providers_proc.csv")
raw_materials = safe_read_csv("raw_materials_proc.csv")
recipes = safe_read_csv("recipes_proc.csv")
sales = safe_read_csv("sales_transactions_proc.csv")  # note the actual name

# ------------------- DIMENSIONS -------------------

# dim_customer
if not customers.empty:
    dim_customer = customers[['customer_id', 'name', 'location_city', 'location_country']]
    dim_customer.to_csv(os.path.join(WAREHOUSE, "dim_customer.csv"), index=False)

# dim_provider
if not providers.empty:
    dim_provider = providers
    dim_provider.to_csv(os.path.join(WAREHOUSE, "dim_provider.csv"), index=False)

# dim_ingredient
if not ingredients.empty:
    dim_ingredient = ingredients
    dim_ingredient.to_csv(os.path.join(WAREHOUSE, "dim_ingredient.csv"), index=False)

# dim_raw_material
if not raw_materials.empty:
    dim_raw_material = raw_materials
    dim_raw_material.to_csv(os.path.join(WAREHOUSE, "dim_raw_material.csv"), index=False)

# dim_flavour → latest batch per flavour_id
if not flavours.empty:
    dim_flavour = flavours.sort_values("batch_number", ascending=False).drop_duplicates("flavour_id")
    dim_flavour.to_csv(os.path.join(WAREHOUSE, "dim_flavour.csv"), index=False)

# dim_recipe
if not recipes.empty:
    dim_recipe = recipes[['recipe_id', 'heat_process', 'yield']]
    dim_recipe.to_csv(os.path.join(WAREHOUSE, "dim_recipe.csv"), index=False)

# dim_date from sales
if not sales.empty:
    sales['transaction_date'] = pd.to_datetime(sales['transaction_date'], errors='coerce')
    dim_date = sales[['transaction_date']].dropna().drop_duplicates()
    dim_date['year'] = dim_date['transaction_date'].dt.year
    dim_date['month'] = dim_date['transaction_date'].dt.month
    dim_date['quarter'] = dim_date['transaction_date'].dt.quarter
    dim_date.to_csv(os.path.join(WAREHOUSE, "dim_date.csv"), index=False)

# ------------------- FACT TABLES -------------------

# fact_recipe_composition
if not recipes.empty:
    fact_recipe_composition = recipes
    fact_recipe_composition.to_csv(os.path.join(WAREHOUSE, "fact_recipe_composition.csv"), index=False)

# fact_sales
if not sales.empty:
    fact_sales = sales
    fact_sales.to_csv(os.path.join(WAREHOUSE, "fact_sales.csv"), index=False)

print("Transformation complete. All dimension and fact tables saved in 'warehouse/'")