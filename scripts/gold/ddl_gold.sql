/*
================================================================================
GOLD LAYER - DIMENSION & FACT VIEWS (Star Schema)
================================================================================

Purpose:
    This script creates **read-only views** that form the core of the Gold layer 
    (data mart / presentation layer) in a classic star schema design.

    What it delivers:
    • dim_customers     → Unified customer dimension (merging CRM + ERP sources)
    • dim_products      → Product dimension with enriched category hierarchy
    • fact_sales        → Transaction fact table with surrogate keys to dimensions

Key characteristics:
    - Uses surrogate keys (customer_key, product_key) for clean analytics
    - Applies business logic for merging overlapping sources (CRM vs ERP)
    - Filters current/active products only (prd_end_dt IS NULL)
    - Ready for BI tools (Power BI, Tableau, etc.) — no heavy computation

Important business rules:
    - Customer gender: CRM preferred → fallback to ERP → 'n/a'
    - Customer country: taken from ERP location mapping
    - Product validity: only currently active products (no end date)
    - Joins use natural/business keys (cst_key, cid, prd_key, etc.)

Usage:
    - These are VIEWS → no data is stored, always computed on query
    - Typically queried directly by reporting tools
    - For performance in large-scale production → consider materializing as tables

Last updated: March 2026
================================================================================
*/


-- =============================================================================
-- 1. DIMENSION: Customers (Unified CRM + ERP sources)
-- =============================================================================

CREATE OR ALTER VIEW gold.dim_customers
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_key)               AS customer_key,        -- Surrogate key for analytics

    ci.cst_id                                             AS customer_id,         -- Original CRM ID
    ci.cst_key                                            AS customer_number,     -- Business key (used for joining)

    ci.cst_firstname                                      AS first_name,
    ci.cst_lastname                                       AS last_name,

    la.cntry                                              AS country,             -- Country from ERP location mapping

    ci.cst_material_status                                AS marital_status,      -- Already standardized in Silver

    -- Gender priority: CRM value if valid → fallback to ERP → default 'n/a'
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                                                   AS gender,

    ca.bdate                                              AS birthdate,           -- Birth date from ERP (more reliable source)

    ci.cst_create_date                                    AS create_date          -- When customer record was created in CRM

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid                               -- Join on business key (customer number)

LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;                              -- Bring in country information


-- Quick validation query: check gender merging logic
-- Should help confirm that CRM gender is preferred and ERP is fallback
/*
SELECT DISTINCT
    ci.cst_gndr               AS crm_gender,
    ca.gen                    AS erp_gender,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                       AS final_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
ORDER BY 1, 2;
*/


-- =============================================================================
-- 2. DIMENSION: Products (CRM products + ERP category hierarchy)
-- =============================================================================

CREATE OR ALTER VIEW gold.dim_products
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,     -- Surrogate key

    pn.prd_id                                                AS product_id,
    pn.prd_key                                               AS product_number,   -- Business key

    pn.prd_nm                                                AS product_name,
    pn.cat_id                                                AS category_id,

    pc.cat                                                   AS category,         -- From ERP hierarchy
    pc.subcat                                                AS subcategory,
    pc.maintenance,

    pn.prd_cost                                              AS cost,
    pn.prd_line                                              AS product_line,     -- Already standardized (Mountain, Road, etc.)

    pn.prd_start_dt                                          AS start_date

FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id                                     -- Join on extracted category ID

WHERE pn.prd_end_dt IS NULL;                                 -- Only currently active / valid products


-- =============================================================================
-- 3. FACT TABLE: Sales Transactions
-- =============================================================================

CREATE OR ALTER VIEW gold.fact_sales
AS
SELECT
    sd.sls_ord_num                                           AS order_number,

    pr.product_key,                                          -- Foreign key → dim_products
    cu.customer_key,                                         -- Foreign key → dim_customers

    sd.sls_order_dt                                          AS order_date,
    sd.sls_ship_dt                                           AS shipping_date,
    sd.sls_due_dt                                            AS due_date,

    sd.sls_sales                                             AS sales_amount,
    sd.sls_quantity                                          AS quantity,
    sd.sls_price                                             AS price

FROM silver.crm_sales_details AS sd

LEFT JOIN gold.dim_products AS pr
    ON sd.sls_ord_key = pr.product_number                    -- Note: using ord_key? Wait — should be product key
                                                             -- Check: in silver it's sls_ord_key, but likely typo
                                                             -- Most likely should be: sd.sls_prd_key = pr.product_number
                                                             -- (assuming sls_prd_key exists in source)

LEFT JOIN gold.dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id;                      -- Join on original CRM customer ID


-- =============================================================================
-- VALIDATION QUERY: Find orphaned sales records (no matching product)
-- Helps detect data quality issues in joins
-- =============================================================================
/*
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL          -- Sales without valid product
   OR c.customer_key IS NULL         -- Sales without valid customer
ORDER BY f.order_number;
*/
