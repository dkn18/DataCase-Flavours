import pandas as pd
import os

# ------------------- Paths -------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROC = os.path.join(SCRIPT_DIR, "..", "processed")
WAREHOUSE = os.path.join(SCRIPT_DIR, "..", "warehouse")
REJECTED = os.path.join(SCRIPT_DIR, "..", "rejected")

os.makedirs(WAREHOUSE, exist_ok=True)
os.makedirs(REJECTED, exist_ok=True)

# ------------------- Helper Functions -------------------

def safe_read_proc(name):
    """Read a processed CSV file safely. Returns empty DataFrame if missing."""
    file_name = f"{name}_proc.csv"
    path = os.path.join(PROC, file_name)
    if os.path.exists(path):
        df = pd.read_csv(path)
        print(f"Loaded {file_name}")
        return df
    else:
        print(f"Warning: {file_name} not found in processed folder.")
        return pd.DataFrame()


def quarantine(df, condition, file_name, reason):
    """Move rows meeting condition to rejected folder."""
    bad = df[condition].copy()
    if not bad.empty:
        bad["rejection_reason"] = reason
        path = os.path.join(REJECTED, file_name)
        bad.to_csv(path, mode="a", index=False, header=not os.path.exists(path))
        print(f"Quarantined {len(bad)} rows to {file_name} ({reason})")
    return df[~condition]


# ------------------- Load all datasets -------------------

datasets = ["customers", "flavours", "ingredients", "providers",
            "raw_materials", "recipes", "sales_transactions"]

data = {name: safe_read_proc(name) for name in datasets}

customers = data["customers"]
flavours = data["flavours"]
ingredients = data["ingredients"]
providers = data["providers"]
raw_materials = data["raw_materials"]
recipes = data["recipes"]
sales = data["sales_transactions"]

# =================================================
# ================= DIMENSIONS ====================
# =================================================

# -------- dim_customer --------
if not customers.empty:
    dim_customer = customers[['customer_id', 'name', 'location_city', 'location_country']]
    dim_customer = quarantine(dim_customer, dim_customer["customer_id"].isna(),
                              "dim_customer_rejected.csv", "missing_customer_id")
    dim_customer = dim_customer.fillna("Unknown")
    dim_customer.to_csv(os.path.join(WAREHOUSE, "dim_customer.csv"), index=False)

# -------- dim_provider --------
if not providers.empty:
    dim_provider = quarantine(providers, providers["provider_id"].isna(),
                              "dim_provider_rejected.csv", "missing_provider_id")
    dim_provider = dim_provider.fillna("Unknown")
    dim_provider.to_csv(os.path.join(WAREHOUSE, "dim_provider.csv"), index=False)

# -------- dim_ingredient --------
if not ingredients.empty:
    dim_ingredient = quarantine(ingredients, ingredients["ingredient_id"].isna(),
                                "dim_ingredient_rejected.csv", "missing_ingredient_id")
    dim_ingredient = dim_ingredient.fillna("Unknown")
    dim_ingredient.to_csv(os.path.join(WAREHOUSE, "dim_ingredient.csv"), index=False)

# -------- dim_raw_material --------
if not raw_materials.empty:
    dim_raw = quarantine(raw_materials, raw_materials["raw_material_id"].isna(),
                         "dim_raw_material_rejected.csv", "missing_raw_material_id")
    dim_raw = dim_raw.fillna("Unknown")
    dim_raw.to_csv(os.path.join(WAREHOUSE, "dim_raw_material.csv"), index=False)

# -------- dim_flavour (latest batch) --------
if not flavours.empty:
    dim_flavour = flavours.sort_values("batch_number", ascending=False)\
                          .drop_duplicates("flavour_id")
    dim_flavour = quarantine(dim_flavour, dim_flavour["flavour_id"].isna(),
                             "dim_flavour_rejected.csv", "missing_flavour_id")
    dim_flavour = dim_flavour.fillna("Unknown")
    dim_flavour.to_csv(os.path.join(WAREHOUSE, "dim_flavour.csv"), index=False)

# -------- dim_recipe --------
if not recipes.empty:
    dim_recipe = recipes[['recipe_id', 'heat_process', 'yield']]
    dim_recipe = quarantine(dim_recipe, dim_recipe["recipe_id"].isna(),
                            "dim_recipe_rejected.csv", "missing_recipe_id")
    dim_recipe = dim_recipe.fillna("Unknown")
    dim_recipe.to_csv(os.path.join(WAREHOUSE, "dim_recipe.csv"), index=False)

# -------- dim_date --------
if not sales.empty:
    sales["transaction_date"] = pd.to_datetime(sales["transaction_date"], errors="coerce")

    bad_dates = sales["transaction_date"].isna()
    if bad_dates.any():
        quarantine(sales, bad_dates, "dim_date_rejected.csv", "invalid_date")

    dim_date = sales[~bad_dates][["transaction_date"]].drop_duplicates()
    dim_date["year"] = dim_date["transaction_date"].dt.year
    dim_date["month"] = dim_date["transaction_date"].dt.month
    dim_date["quarter"] = dim_date["transaction_date"].dt.quarter
    dim_date.to_csv(os.path.join(WAREHOUSE, "dim_date.csv"), index=False)

# =================================================
# =================== FACTS =======================
# =================================================

# -------- fact_recipe_composition --------
if not recipes.empty:
    fact_recipe = quarantine(
        recipes,
        recipes[["recipe_id", "raw_material_id", "flavour_id", "ingredient_id"]].isna().any(axis=1),
        "fact_recipe_rejected.csv",
        "missing_required_key"
    )
    fact_recipe.to_csv(os.path.join(WAREHOUSE, "fact_recipe_composition.csv"), index=False)

# -------- fact_sales --------
if not sales.empty:
    fact_sales = quarantine(
        sales,
        sales[["transaction_id", "customer_id", "flavour_id"]].isna().any(axis=1),
        "fact_sales_rejected.csv",
        "missing_required_key"
    )
    fact_sales = fact_sales.fillna({"transaction_country": "Unknown"})
    fact_sales.to_csv(os.path.join(WAREHOUSE, "fact_sales.csv"), index=False)

print("\nTransformation complete.")
print("Warehouse tables → warehouse/")
print("Rejected records → rejected/")