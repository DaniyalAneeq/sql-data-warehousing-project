/*
================================================================================
SILVER LAYER - MASTER LOAD PROCEDURE
================================================================================

Procedure:  silver.load_silver

Purpose:
    This is the **central orchestration procedure** for loading the entire Silver layer.
    It performs a **full refresh** of all silver tables by:
    1. Truncating each target table
    2. Inserting cleansed, standardized, and enriched data from the corresponding bronze table
    3. Applying all necessary transformations (TRIM, CASE mappings, date conversions, LEAD logic, etc.)
    4. Measuring and logging duration for each table and the overall batch

Tables processed (in this order):
    CRM sources:
    • silver.crm_cust_info
    • silver.crm_prd_info
    • silver.crm_sales_details
    ERP sources:
    • silver.erp_cust_az12
    • silver.erp_loc_a101
    • silver.erp_px_cat_g1v2

Execution characteristics:
    - Full truncate + load → not incremental
    - Very verbose PRINT logging (good for development / debugging)
    - Basic TRY-CATCH block for error visibility
    - No transaction control (each INSERT is auto-committed)

Typical usage:
    EXEC silver.load_silver;

Important notes / production recommendations:
    - Currently uses full refresh → data loss on re-run
    - In production consider:
      - Adding proper logging to a table instead of PRINT
      - Using transactions or checkpoints per table
      - Making it incremental (if volume grows)
      - Adding email/alert on failure
      - Parameterizing behavior (truncate vs merge, etc.)
    - Error handling is minimal — consider THROW; or RAISERROR with severity

Last updated: March 2026
================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time       DATETIME,
            @end_time         DATETIME,
            @batch_start_time DATETIME = GETDATE(),
            @batch_end_time   DATETIME;

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Silver Layer - Full Refresh';
        PRINT '================================================';
        PRINT 'Batch started: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT '';

        -- ────────────────────────────────────────────────
        -- CRM Sources
        -- ────────────────────────────────────────────────
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';
        PRINT '';

        -- crm_cust_info ───────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname)                 AS cst_firstname,        -- Remove leading/trailing spaces
            TRIM(cst_lastname)                  AS cst_lastname,         -- Remove leading/trailing spaces
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END                                 AS cst_material_status,  -- Standardize marital status
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END                                 AS cst_gndr,             -- Standardize gender
            cst_create_date
        FROM (
            -- Deduplication: keep only the most recent record per customer
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- crm_prd_info ────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  AS cat_id,           -- Extract category code
            SUBSTRING(prd_key, 7, LEN(prd_key))          AS prd_key,          -- Extract product identifier
            prd_nm,
            COALESCE(prd_cost, 0)                        AS prd_cost,         -- Replace NULL cost with 0
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END                                          AS prd_line,         -- Standardize product line
            CAST(prd_start_dt AS DATE)                   AS prd_start_dt,
            -- Enrich: calculate end date using LEAD (valid-to date)
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- crm_sales_details ───────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_ord_key,           -- corrected name (was sls_prd_key in some earlier versions)
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_ord_key,
            sls_cust_id,
            -- Convert YYYYMMDD integer to DATE (invalid → NULL)
            CASE WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 
                 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 
                 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END   AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 
                 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END    AS sls_due_dt,

            -- Repair inconsistent sales amount
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 
                      OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(NULLIF(sls_price, 0))
                 ELSE sls_sales END                                      AS sls_sales,

            sls_quantity,

            -- Derive price when invalid
            CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN
                CASE WHEN sls_quantity > 0 AND sls_sales > 0
                     THEN sls_sales / NULLIF(sls_quantity, 0)
                     ELSE NULL END
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- ────────────────────────────────────────────────
        -- ERP Sources
        -- ────────────────────────────────────────────────
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';
        PRINT '';

        -- erp_cust_az12 ───────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,  -- Remove legacy 'NAS' prefix
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END                          AS bdate,  -- No future birth dates
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END                                                                               AS gen     -- Standardize gender
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- erp_loc_a101 ────────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,  -- Remove dashes from ID
            CASE 
                WHEN TRIM(cntry) = 'DE'               THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA')     THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- erp_px_cat_g1v2 ─────────────────────────────────
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        -- Very light transformation — mostly pass-through
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '';


        -- ────────────────────────────────────────────────
        -- Batch Summary
        -- ────────────────────────────────────────────────
        SET @batch_end_time = GETDATE();

        PRINT '==========================================';
        PRINT 'Loading Silver Layer Completed Successfully';
        PRINT '==========================================';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT 'Batch start: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT 'Batch end:   ' + CONVERT(NVARCHAR, @batch_end_time, 120);
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING SILVER LAYER LOAD';
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER()  AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE()   AS NVARCHAR);
        PRINT 'Error Line   : ' + CAST(ERROR_LINE()    AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '==========================================';
        -- Optional: add THROW; to propagate error to caller
        -- THROW;
    END CATCH
END;
GO
