/*
============================================================
-- Checking & Testing
=============================================================
*/

--------------------- crm_cust_info -------------------------
-- Check for nulls and duplicates in Primary Key
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for white spaces in string values
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Check for inconsistent data
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

--------------------- crm_prd_info -------------------------
-- Check for nulls and duplicates in Primary Key
SELECT prd_id, COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Extract a specific part of the string value
SELECT SUBSTRING(prd_key, 1, 5) AS cat_id
FROM bronze.crm_prd_info
WHERE prd_key IS NOT NULL;

-- Check for invalid date order
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

------------------ crm_sales_details -----------------------
-- Check for white spaces in sls_ord_num
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check for invalid date formats (convert 0 to null)
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Sales = Quntity * Price
-- Negative, zero, nulls are not allowed
SELECT
  sls_sales as old_sls_sales,
  sls_quantity,
  sls_price as old_sls_price,
  CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
    END AS sls_sales,
  CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
    ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
  OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0;

------------------ erp_loc_a101 -----------------------------
-- Check for white spaces in Gender values in erp_cust_az12
SELECT DISTINCT
  UPPER(REPLACE(REPLACE(REPLACE(TRIM(gen), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')) AS cleaned_gen,
  LEN(REPLACE(REPLACE(REPLACE(TRIM(gen), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')) AS cleaned_length
FROM bronze.erp_cust_az12;