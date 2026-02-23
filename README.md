# DataCase-Flavours

**Overview**

This project implements a complete ETL pipeline that ingests raw CSV data, performs data cleaning and quality checks, and produces warehouse-ready dimension and fact tables for analytics and dashboards.

**Key features:**

- Multi-source raw data ingestion

- Automated data quality checks (nulls, duplicates, invalid dates)

- Dimensional modeling (Dimensions & Facts)

- Quarantine of invalid rows

- Output CSVs ready for dashboarding

**Folder Structure**

--DataCase-Flavours

        --raw/                       # Original CSVs
    
        --ingestion/ingest.py        # Ingest raw → processed
    
        --processed/                 # Cleaned files + audit logs (_proc.csv)
    
        --transform/transform.py     # Transform processed → warehouse
    
        --warehouse/                 # Dimension & fact tables 
    
--run_pipeline.py  # Orchestrates ingestion + transformation

--requirements.txt

--README.md


**Pipeline Layers**

1. **Raw Layer** - No transformations applied
                Original CSVs (landing zone)

2. **Processed Layer** - Cleansed, deduplicated files with audit logs
                Primary keys checked; invalid rows quarantined to rejected folder(the folder will be automatically created if such rows found)
                Output: _proc.csv files in processed/
                
3. **Transformation Layer**
        
   **Dimensions:**

   **Table**	    ||    **Key Columns / Notes**
   
   dim_customer	    ||    customer_id, name, location_city, location_country
   
   dim_provider	    ||    provider_id, name, country
   
   dim_ingredient   ||	  ingredient_id, name
   
   dim_raw_material ||	raw_material_id, name
   
   dim_flavour	    ||    flavour_id, latest batch description
   
   dim_recipe	    ||    recipe_id, heat_process, yield
   
   dim_date	    ||    Derived from sales dates (transaction_date, year, month, quarter)
   

   **Fact tables:**

   **Table**	               ||        **Description**
                
   fact_recipe_composition     ||        Recipe ingredient composition
   
   fact_sales	               ||        Sales transactions


4. **Running the Pipeline**
      1. Install dependencies: pip install -r requirements.txt
      2. Place raw CSVs in the raw/ folder. (exists already)
      3. Run the full ETL pipeline: python run_pipeline.py
               -Ingests raw → processed (with audit logs)

               -Transforms processed → warehouse (dimensions & facts)

               -Quarantined rows appear in rejected/ if exists
                
**Data quality:**

- Mandatory keys missing → quarantined
- Descriptive fields null → replaced with "Unknown"

**Notes**

- Dynamic file handling: The pipeline automatically picks up all _proc.csv files for transformation.

- Flexible and maintainable: New datasets can be added without changing code.

- Traceability: Rejected records and audit logs ensure full visibility into data quality.

**Summary:**
This ETL pipeline ensures clean, auditable, and analytics-ready data from raw CSVs to warehouse-ready dimensions and fact tables, while handling nulls, duplicates, and invalid data gracefully.
