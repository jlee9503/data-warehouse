/*
=============== Full Loads ================
    -- Status Messages using PRINT
    -- Error Handling using TRY...CATCH (Better debugging experience)
    -- Track ETL Duration (monitor trends, detect issues, optimze performance)
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.bronze_load
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
    -- This procedure loads data from CSV files into the bronze schema tables.
    -- It uses BULK INSERT to load data from the specified file paths.

    SET @batch_start_time = GETDATE();
    PRINT '========================================';
    PRINT 'Loading Bronze Layer';
    PRINT '========================================';

    PRINT '----------------------------------------';
    PRINT 'Loading CRM Data';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating tables: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>>Inserting data into bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT 'Truncating tables: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT 'Inserting data into bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT 'Truncating tables: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT 'Inserting data into bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    PRINT '----------------------------------------';
    PRINT 'Loading ERP Data';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating tables: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>>Inserting data into bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/datasets/source_erp/loc_a101.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating tables: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>>Inserting data into bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/datasets/source_erp/cust_az12.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '----------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating tables: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>>Inserting data into bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/datasets/source_erp/px_cat_g1v2.csv'
    WITH
    (
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    
    SET @batch_end_time = GETDATE();
    PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '----------------------------------------';
    PRINT 'Loading Bronze Layer Completed';
    END TRY

    BEGIN CATCH
        PRINT '=========================================';
        PRINT 'Error occurred during the load process.';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '==========================================';
    END CATCH
END