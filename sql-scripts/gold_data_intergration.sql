-- Customer Information (Dimension)
-- Data Integration:
  -- Gender (cst_gndr and gen)
    -- What is the master data? (Should ask source expert about it)
    -- In this project, we will take CRM as the master data
-- Rename columns to meaningful names
-- Surrogate key: customer_id
CREATE VIEW gold.dim_customer AS
  SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, 
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ca.bdate AS birth_date,
    CASE WHEN ci.cst_gndr != 'Unknown' THEN cst_gndr
        ELSE COALESCE(ca.gen, 'Unknown')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    lo.cntry AS country,
    ci.cst_created_date AS create_date
  FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
      ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 lo
      ON ci.cst_key = lo.cid;

GO

-- Product Information (Dimension)
  -- Filter out all hislsical data (only select current product)
  -- Rename columns to meaningful names
  -- Surrogate key: product_id
CREATE VIEW gold.dim_product AS
  SELECT
    ROW_NUMBER() OVER (ORDER BY pd.prd_start_dt, pd.prd_key) AS product_key,
    pd.prd_id AS product_id,
    pd.prd_key AS product_number,
    pd.prd_nm AS product_name,
    pd.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pd.prd_cost AS product_cost,
    pd.prd_line AS product_line,
    pd.prd_start_dt AS product_start_date
  FROM silver.crm_prd_info pd
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pd.cat_id = pc.id
  WHERE pd.prd_end_dt IS NULL;
GO

-- Sales Information (Transaction)
  -- Rename columns to meaningful names
  -- Sort columns into logical groups to improve readability
  -- Connecting multiple dimensions:
    -- 1. Keys
    -- 2. Dates
    -- 3. Measures
  -- Surrogate key: sls_prd_key, sls_cust_id
CREATE VIEW gold.fact_sales AS
  SELECT
    st.sls_ord_num AS order_number,
    pd.product_key,
    cu.customer_key,
    st.sls_prd_key AS sls_number,
    st.sls_cust_id AS name,
    st.sls_order_dt AS order_date,
    st.sls_ship_dt AS ship_date,
    st.sls_due_dt AS due_date,
    st.sls_sales AS sales,
    st.sls_quantity AS quantity,
    st.sls_price AS price
  FROM silver.crm_sales_details st
    LEFT JOIN gold.dim_product pd
      ON st.sls_prd_key = pd.product_number
    LEFT JOIN gold.dim_customer cu
      ON st.sls_cust_id = cu.customer_id
GO