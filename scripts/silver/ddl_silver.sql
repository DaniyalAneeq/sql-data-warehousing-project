/*
================================================================================
SILVER LAYER - CLEANSED & LIGHTLY TRANSFORMED TABLES
================================================================================

Purpose:
    This script creates the Silver-layer tables that serve as the cleansed, 
    standardized, and lightly enriched version of the raw Bronze data.

    Silver layer goals:
    - Apply basic data quality rules (data types, null handling, standardization)
    - Perform initial transformations (e.g. date formatting, code mapping, deduplication)
    - Add audit / metadata columns (e.g. dwh_create_date)
    - Act as the "single source of truth" for cleaned data
    - Usually built via stored procedures or ELT pipelines from Bronze
    - Still close to source structure - major business logic / aggregations go to Gold

Tables included (mirroring Bronze sources):
    • silver.crm_cust_info         → Cleaned CRM customer master
    • silver.crm_prd_info          → Cleaned CRM product catalog
    • silver.crm_sales_details     → Cleaned CRM sales transactions
    • silver.erp_loc_a101          → Standardized ERP country/location mapping
    • silver.erp_cust_az12         → Cleaned ERP customer master (alternative source)
    • silver.erp_px_cat_g1v2       → Standardized ERP product category hierarchy

Key additions in Silver (compared to Bronze):
    • dwh_create_date DATETIME2 DEFAULT GETDATE()  → audit timestamp when record was processed/inserted
    • Potential future additions (not in this script yet): 
      - surrogate keys, hash keys (for SCD), valid_from/valid_to (if SCD2), 
      - cleaned/standardized columns (e.g. gender → 'M'/'F'/'Unknown', country codes, etc.)

Usage:
    This script is typically run during:
    - Initial environment setup
    - Schema reset / testing
    - When Silver structure needs to change (new columns, data type adjustments)

Important:
    - Tables are dropped and recreated → this is NOT an incremental operation
    - Actual data population / transformation usually happens in a separate 
      stored procedure (e.g. silver.load_silver )
    - This script only defines structure — no data is inserted here

Last updated: March 2026
================================================================================
*/

-- ────────────────────────────────────────────────
-- CRM Customer Information (cleaned)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),    -- marital status (to be standardized in transform proc)
    cst_gndr            NVARCHAR(50),    -- gender (to be standardized: M/F/Unknown/...)
    cst_create_date     DATE,
    
    -- Metadata / audit column
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO


-- ────────────────────────────────────────────────
-- CRM Product Information (cleaned)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),        -- product line / category
    prd_start_dt    DATETIME,
    prd_end_dt      DATETIME,
    
    -- Metadata / audit column
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ────────────────────────────────────────────────
-- CRM Sales Transactions (cleaned)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_ord_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    INT,                 -- YYYYMMDD integer → usually converted to DATE in transform
    sls_ship_dt     INT,
    sls_due_dt      INT,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    
    -- Metadata / audit column
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ────────────────────────────────────────────────
-- ERP Location / Country mapping (standardized)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),        -- customer / location ID
    cntry           NVARCHAR(50),        -- country (to be standardized / ISO code if needed)
    
    -- Metadata / audit column
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ────────────────────────────────────────────────
-- ERP Customer data – alternative source (cleaned)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),        -- customer ID
    bdate           DATE,                -- birth date
    gen             NVARCHAR(50),        -- gender (to be standardized)
    
    -- Metadata / audit column
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ────────────────────────────────────────────────
-- ERP Product Category Hierarchy (standardized)
-- ────────────────────────────────────────────────
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),        -- main category
    subcat          NVARCHAR(50),        -- subcategory
    maintenance     NVARCHAR(50),        -- maintenance flag / group
    
    -- Metadata / audit column
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
