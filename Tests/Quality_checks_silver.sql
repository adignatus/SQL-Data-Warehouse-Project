/*
=============================================================================
SILVER LAYER – DATA QUALITY CHECKS
=============================================================================
Purpose:
  This script performs data quality validations across the Bronze and Silver
  layers to ensure data consistency, accuracy, and standardization before
  consumption by the Gold layer.

Checks Covered:
  - Duplicate and NULL business keys
  - Unwanted leading/trailing spaces
  - Domain standardization
  - Invalid date values and date ordering
  - Cross-field consistency (Sales = Quantity * Price)
  - Referential readiness for downstream joins

Usage Notes:
  - Execute AFTER loading the Silver layer.
  - All queries are diagnostic: NO data is modified.
  - Any returned rows indicate data quality issues requiring investigation.
=============================================================================
*/

--------------------------------------------------------------------------------
-- BRONZE → CRM CUSTOMER INFO
--------------------------------------------------------------------------------

/* Duplicate or NULL customer IDs */
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

/* Unwanted spaces */
SELECT cst_firstname FROM bronze.crm_cust_info WHERE cst_firstname <> TRIM(cst_firstname);
SELECT cst_lastname  FROM bronze.crm_cust_info WHERE cst_lastname  <> TRIM(cst_lastname);
SELECT cst_gndr      FROM bronze.crm_cust_info WHERE cst_gndr      <> TRIM(cst_gndr);
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status <> TRIM(cst_marital_status);

/* Domain inspection */
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

--------------------------------------------------------------------------------
-- SILVER → CRM CUSTOMER INFO (POST-CLEANING VALIDATION)
--------------------------------------------------------------------------------

/* Expectation: No unwanted spaces */
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname <> TRIM(cst_firstname);
SELECT cst_lastname  FROM silver.crm_cust_info WHERE cst_lastname  <> TRIM(cst_lastname);

/* Domain consistency */
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

/* Load validation */
SELECT COUNT(*) AS row_count FROM silver.crm_cust_info;
SELECT * FROM silver.crm_cust_info;

--------------------------------------------------------------------------------
-- SILVER → CRM PRODUCT INFO
--------------------------------------------------------------------------------

/* Duplicate or NULL product IDs */
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

/* Unwanted spaces */
SELECT prd_nm FROM silver.crm_prd_info WHERE prd_nm <> TRIM(prd_nm);

/* Invalid or negative costs */
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

/* Domain inspection */
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

/* Invalid date ranges */
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * FROM silver.crm_prd_info;

--------------------------------------------------------------------------------
-- BRONZE → CRM SALES DETAILS
--------------------------------------------------------------------------------

/* Unwanted spaces in keys */
SELECT * FROM bronze.crm_sales_details WHERE sls_ord_num  <> TRIM(sls_ord_num);
SELECT * FROM bronze.crm_sales_details WHERE sls_prd_key  <> TRIM(sls_prd_key);
SELECT * FROM bronze.crm_sales_details WHERE sls_cust_id  <> TRIM(sls_cust_id);

/* Invalid order dates */
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0
   OR LEN(sls_order_dt) <> 8
   OR sls_order_dt < 19000101
   OR sls_order_dt > 20300101;

/* Invalid date sequencing */
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

/* Sales consistency check */
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales <> sls_quantity * ABS(sls_price)
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

--------------------------------------------------------------------------------
-- SILVER → CRM SALES DETAILS (POST-CLEANING VALIDATION)
--------------------------------------------------------------------------------

/* Expectation: Sales = Quantity * Price */
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL;

/* Date order validation */
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

SELECT * FROM silver.crm_sales_details;

--------------------------------------------------------------------------------
-- BRONZE → ERP CUSTOMER (AZ12)
--------------------------------------------------------------------------------

/* Unwanted spaces */
SELECT * FROM bronze.erp_cust_az12 WHERE cid <> TRIM(cid);

/* CID normalization check */
SELECT cid
FROM bronze.erp_cust_az12
WHERE
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN (SELECT cst_key FROM silver.crm_cust_info);

/* Invalid birthdates */
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01'
   OR bdate > GETDATE();

--------------------------------------------------------------------------------
-- BRONZE → ERP LOCATION (A101)
--------------------------------------------------------------------------------

/* Unwanted spaces */
SELECT * FROM bronze.erp_loc_a101 WHERE cid <> TRIM(cid);

/* Key alignment with CRM */
SELECT REPLACE(cid, '-', '') AS cleaned_cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info);

/* Domain inspection */
SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry;
SELECT * FROM silver.erp_loc_a101;

--------------------------------------------------------------------------------
-- BRONZE → ERP PRODUCT CATEGORY
--------------------------------------------------------------------------------

/* Unwanted spaces */
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE id <> TRIM(id);
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2 WHERE cat <> TRIM(cat);
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2 WHERE subcat <> TRIM(subcat);
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2 WHERE maintenance <> TRIM(maintenance);

/* Silver validation */
SELECT * FROM silver.erp_px_cat_g1v2;

--------------------------------------------------------------------------------
-- END OF SILVER LAYER QUALITY CHECKS
--------------------------------------------------------------------------------
