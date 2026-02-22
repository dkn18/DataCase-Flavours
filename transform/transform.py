import pandas as pd
import os

# ------------------ Setup Paths ------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # Current folder
PROC = os.path.join(SCRIPT_DIR, "..", "processed")      # Processed folder (one level up)
WAREHOUSE = os.path.join(SCRIPT_DIR, "warehouse")       # Warehouse folder
os.makedirs(WAREHOUSE, exist_ok=True)

# ------------------ Load Processed Files ------------------
dfs = {}  # Dictionary to hold all processed dataframes

# Loop over all CSVs in processed folder
for file in os.listdir(PROC):
    if file.endswith("_proc.csv"):
        path = os.path.join(PROC, file)
        key_name = file.replace("_proc.csv", "")
        try:
            dfs[key_name] = pd.read_csv(path)
            print(f"Loaded {file} as '{key_name}'")
        except Exception as e:
            print(f"Failed to load {file}: {e}")

# ------------------ Helper: safe get ------------------
# Returns empty dataframe if a processed file is missing
def get_df(name):
    return dfs.get(name, pd.DataFrame())

# ------------------ Build DIMENSIONS ------------------
customers = get_df("customers")
if not customers.empty:
    dim_customer = customers[['customer_id', 'name', 'location_city', 'location_country']]
    dim_customer.to_csv(os.path.join(WAREHOUSE, "dim_customer.csv"), index=False)

providers = get_df("providers")
if not providers.empty:
    dim_provider = providers
    dim_provider.to_csv(os.path.join(WAREHOUSE, "dim_provider.csv"), index=False)

ingredients = get_df("ingredients")
if not ingredients.empty:
    dim_ingredient = ingredients
    dim_ingredient.to_csv(os.path.join(WAREHOUSE, "dim_ingredient.csv"), index=False)

raw_materials = get_df("raw_materials")
if not raw_materials.empty:
    dim_raw_material = raw_materials
    dim_raw_material.to_csv(os.path.join(WAREHOUSE, "dim_raw_material.csv"), index=False)

flavours = get_df("flavours")
if not flavours.empty:
    dim_flavour = flavours.sort_values("batch_number", ascending=False).drop_duplicates("flavour_id")
    dim_flavour.to_csv(os.path.join(WAREHOUSE, "dim_flavour.csv"), index=False)

recipes = get_df("recipes")
if not recipes.empty:
    dim_recipe = recipes[['recipe_id', 'heat_process', 'yield']]
    dim_recipe.to_csv(os.path.join(WAREHOUSE, "dim_recipe.csv"), index=False)

sales = get_df("sales_transaction")
if not sales.empty:
    sales['transaction_date'] = pd.to_datetime(sales['transaction_date'], errors='coerce')
    dim_date = sales[['transaction_date']].dropna().drop_duplicates()
    dim_date['year'] = dim_date['transaction_date'].dt.year
    dim_date['month'] = dim_date['transaction_date'].dt.month
    dim_date['quarter'] = dim_date['transaction_date'].dt.quarter
    dim_date.to_csv(os.path.join(WAREHOUSE, "dim_date.csv"), index=False)

# ------------------ Build FACT TABLES ------------------
if not recipes.empty:
    fact_recipe_composition = recipes
    fact_recipe_composition.to_csv(os.path.join(WAREHOUSE, "fact_recipe_composition.csv"), index=False)

if not sales.empty:
    fact_sales = sales
    fact_sales.to_csv(os.path.join(WAREHOUSE, "fact_sales.csv"), index=False)

print("Transformation complete. Dimensions and fact tables are saved in 'warehouse/'")
