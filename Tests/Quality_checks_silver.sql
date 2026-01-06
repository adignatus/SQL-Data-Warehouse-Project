/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data Toading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/

=============================================================================
  Checking: bronze.crm_cust_info
=============================================================================
-- Quality Issues checks on bronze.crm_cust_info
  
-- Checking available duplicate and possible Nulls
SELECT 
	cst_id,
	COUNT (*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1
  
-- Check for unwanted spaces
SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM (cst_firstname)

SELECT 
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM (cst_lastname)

SELECT 
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM (cst_gndr)

SELECT 
	cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM (cst_marital_status)

-- Data Standardization & Consistency 
SELECT DISTINCT 
	cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT 
	cst_marital_status
FROM bronze.crm_cust_info

------------------------------------
-- Quality checks after cleaning
------------------------------------
-- Data Quality Checks and vaildation on Silver.crm_cust_info after Loading  
SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM (cst_firstname)

SELECT 
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM (cst_lastname)
  
-- Data Standardization & Consistency
SELECT DISTINCT 
	cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT 
	cst_marital_status
FROM silver.crm_cust_info

-- Silver Table Data Transformation & Loads Confirmation/ Validation
SELECT 
*
FROM silver.crm_cust_info

-- Check exact No. of Rows Loaded from Bronze Layer
SELECT 
COUNT (*)
FROM silver.crm_cust_info

=============================================================================
  Checking: silver.crm_prd_info
=============================================================================

-- QUALITY CHECKS
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT
*
FROM silver.crm_prd_info

=============================================================================
  Checking: bronze.crm_sales_details
=============================================================================

  -- Checking unwanted spaces
SELECT 
	*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM (sls_ord_num)


SELECT 
	*
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM (sls_prd_key)

SELECT 
	*
FROM bronze.crm_sales_details
WHERE sls_cust_id != TRIM (sls_cust_id)

-- Checking invalid date on sls_order_dt column
SELECT 
	NULLIF (sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <0 OR 
LEN (sls_order_dt) != 8 OR
sls_order_dt < 19000101 OR
sls_order_dt > 20300101

-- Checking invalid date 
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_order_dt >  sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * ABS (sls_price)
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- data quality Checks after inserting to the Table 
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULl OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Check for Invalid Date Orders
SELECT

FROM silver.crm_sales_details
WHERE sls_order dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Testing workdone
SELECT 
*
FROM silver.crm_sales_details

=============================================================================
  Checking: bronze.erp_cust_az12
=============================================================================

SELECT *
FROM bronze.erp_cust_az12
WHERE cid != TRIM (cid)

-- Checking the table to be connected to if they have correlated keys or IDs to enable us join them
SELECT * 
FROM silver.crm_cust_info

-- To know if there cid that do not start with NAS
SELECT 
	*,
	COUNT (cid) OVER () -- to know the number 
FROM bronze.erp_cust_az12
WHERE cid NOT LIKE 'NAS%'


SELECT 
	cid,
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid)) 
		ELSE cid 
	END AS cid
	,COUNT (cid) OVER ()
FROM bronze.erp_cust_az12
-- confirm if all of the cid are available in the silver.crm_cust_info
WHERE 
  CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid)) 
		ELSE cid 
	END
	NOT IN (SELECT cst_key FROM silver.crm_cust_info) 


SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE () OR LEN (bdate) != 10


=============================================================================
  Checking: bronze.erp_cust_az12
=============================================================================

SELECT *
FROM bronze.erp_loc_a101
WHERE cid NOT LIKE 'AW%'

SELECT *
FROM bronze.erp_loc_a101
WHERE cid != TRIM (cid)


SELECT 
cid,
COUNT (*) OVER ()
FROM bronze.erp_loc_a101
--WHERE cid NOT IN 
(SELECT cst_key FROM silver.crm_cust_info)

SELECT 
cid,
COUNT (*) OVER ()
FROM bronze.erp_loc_a101
WHERE cid  IN 
(SELECT cst_key FROM silver.crm_cust_info)

SELECT 
REPLACE (cid, '-','') cid
FROM bronze.erp_loc_a101


SELECT 
REPLACE (cid, '-','') cid,
COUNT (*) OVER ()
FROM bronze.erp_loc_a101
WHERE REPLACE (cid, '-','')  NOT IN 
(SELECT cst_key FROM silver.crm_cust_info)


-- Data Quality Checks
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101


--Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

=============================================================================
  Checking: bronze.erp_cust_az12
=============================================================================
-- chcking for quality issues
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM (id)

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM (cat)

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM (subcat)

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM (maintenance)


-- Nothing to be cleaned on the bronze.erp_px_cat_g1v2 table so have to load it into the silver.erp_px_cat_g1v2
-- No table Update as since we didn't add or change any column data type...

-- testing the Table
SELECT *
FROM silver.erp_px_cat_g1v2
-----------------------------------------------------------------------------
