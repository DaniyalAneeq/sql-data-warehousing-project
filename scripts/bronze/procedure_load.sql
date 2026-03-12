/*
================================================================================
BRONZE LAYER - DATA LOADING PROCEDURE
================================================================================

Procedure:  bronze.load_bronze

Purpose:
    This stored procedure is responsible for (re)loading all raw source data 
    into the Bronze-layer tables from local CSV files.

    What it does:
    - Truncates each Bronze table (removes all existing data)
    - Uses BULK INSERT to efficiently load data from CSV files
    - Measures and prints duration for each table load
    - Prints overall batch summary (total time, start/end timestamps)
    - Includes basic error handling with CATCH block

    Characteristics:
    - Full refresh (truncate + load) — not incremental
    - Uses local file paths → intended for development / local testing
    - Hard-coded file paths — in production this would typically be parameterized
      or replaced with external table / Azure Data Factory / SSIS / copy activity
    - Very verbose logging via PRINT statements (helpful during debugging)

Tables loaded (in this order):
    CRM:
    • bronze.crm_cust_info
    • bronze.crm_prd_info
    • bronze.crm_sales_details
    ERP:
    • bronze.erp_loc_a101
    • bronze.erp_cust_az12
    • bronze.erp_px_cat_g1v2

Typical usage:
    EXEC bronze.load_bronze;

Important notes:
    - Requires BULK INSERT permissions
    - CSV files must exist at the exact paths specified
    - Assumes CSVs have headers (skips FIRSTROW = 2)
    - Uses comma as field terminator (no support for quoted commas yet)
    - No data type validation / cleansing — that's done in Silver layer
    - Local file path makes this suitable only for dev/local SQL Server instances

Recommendations for production:
    - Parameterize file paths or use Azure Blob / Data Lake paths
    - Replace BULK INSERT with PolyBase external tables or ADF pipelines
    - Consider adding logging to a persistent table instead of PRINT
    - Add email / alert on failure
    - Implement incremental load logic if volume grows

Last updated: March 2026
================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time       DATETIME,
            @end_time         DATETIME,
            @batch_start_time DATETIME = GETDATE(),
            @batch_end_time   DATETIME;

    BEGIN TRY
        PRINT '======================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '======================';
        PRINT 'Batch started: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT '';

        -- ────────────────────────────────────────────────
        -- CRM Source Files
        -- ────────────────────────────────────────────────
        PRINT '----------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '----------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';


        -- ────────────────────────────────────────────────
        -- ERP Source Files
        -- ────────────────────────────────────────────────
        PRINT '----------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '----------------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\cloud-data-engineering\projects\playground-projects\sql-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------------------';


        -- ────────────────────────────────────────────────
        -- Batch Summary
        -- ────────────────────────────────────────────────
        SET @batch_end_time = GETDATE();

        PRINT '==========================================';
        PRINT 'BATCH LOAD SUMMARY';
        PRINT '==========================================';
        PRINT '>> TOTAL BRONZE LAYER LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

        DECLARE @total_seconds INT = DATEDIFF(SECOND, @batch_start_time, @batch_end_time);
        DECLARE @minutes INT = @total_seconds / 60;
        DECLARE @seconds INT = @total_seconds % 60;

        IF @minutes > 0
            PRINT '>> TOTAL DURATION (MM:SS): ' + CAST(@minutes AS NVARCHAR) + ' min ' + CAST(@seconds AS NVARCHAR) + ' sec';
        ELSE
            PRINT '>> TOTAL DURATION: ' + CAST(@seconds AS NVARCHAR) + ' seconds';

        PRINT '>> START TIME: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT '>> END TIME:   ' + CONVERT(NVARCHAR, @batch_end_time, 120);
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '========================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line:    ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '========================';
        -- Optional: RAISERROR or THROW to bubble up the error
        -- THROW;
    END CATCH
END;
GO

-- Execution example / test call
-- EXEC bronze.load_bronze;
