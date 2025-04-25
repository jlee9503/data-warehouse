/*
========== Full Loads ==========
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.bronze_load
AS
BEGIN
  -- This procedure loads data from CSV files into the bronze schema tables.
  -- It uses BULK INSERT to load data from the specified file paths.

  -- Load customer information
  TRUNCATE TABLE bronze.crm_cust_info;
  BULK INSERT bronze.crm_cust_info
  FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

  -- Load product information
  TRUNCATE TABLE bronze.crm_prd_info;
  BULK INSERT bronze.crm_prd_info
  FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

  -- Load sales details
  TRUNCATE TABLE bronze.crm_sales_details;
  BULK INSERT bronze.crm_sales_details
  FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

  -- Load ERP location data
  TRUNCATE TABLE bronze.erp_loc_a101;
  BULK INSERT bronze.erp_loc_a101
  FROM '/var/opt/mssql/data/datasets/source_erp/loc_a101.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

  -- Load ERP customer data
  TRUNCATE TABLE bronze.erp_cust_az12;
  BULK INSERT bronze.erp_cust_az12
  FROM '/var/opt/mssql/data/datasets/source_erp/cust_az12.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

  -- Load ERP product category data
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  BULK INSERT bronze.erp_px_cat_g1v2
  FROM '/var/opt/mssql/data/datasets/source_erp/px_cat_g1v2.csv'
  WITH
  (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      TABLOCK
  );

END