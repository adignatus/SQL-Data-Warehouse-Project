/*
==============================================================================
File:        proc_load_silver.sql
Repository:  MrIgis/SQL-Data-Warehouse-Project
Path:        /scripts/Silver/proc_load_silver.sql
Author:      MrIgis
Date:        2026-01-10
Purpose:
  Stored Procedure: Load Silver Layer (Bronze -> Silver)

  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populate the Silver schema tables from the Bronze schema. It is intended to
  be the final documented version of the Silver-layer load job.

Summary of behaviour:
  - Full-refresh style load: TRUNCATE target Silver tables then INSERT transformed
    and cleansed rows from corresponding Bronze tables.
  - Minimal transformations/normalizations are applied (gender, marital status,
    product-key normalization, basic date handling, simple numeric corrections).
  - Execution time per section is printed for operational traceability.

Notes / Preconditions:
  - Procedure does not accept parameters.
  - Requires appropriate permissions to TRUNCATE/INSERT on `silver` schema.
  - This version preserves the original SQL logic and flow; only documentation,
    formatting and non-invasive syntactic cleanups are applied.

Usage:
  EXEC silver.load_silver;
==============================================================================
*/
--------------------------------------------------------------------------------------------------------

-- Update on the SP on the silver layer...
-- Two ddl scripts language were removed from the silver layer SP... (silver.crm_prd_info and silver.crm_sales_details

CREATE OR ALTER PROCEDURE silver.load_silver  AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME 
	BEGIN TRY
	SET @batch_start_time = GETDATE ()
	PRINT '=====================================================';
	PRINT '				Loading Silver Layer';
	PRINT '=====================================================';

	PRINT '--------------------';
	PRINT 'Loading CRM Tables';
	PRINT '--------------------';

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info 
	(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date

	FROM
	(
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
	)t 
	WHERE flag_last = 1 AND cst_id IS NOT NULL

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	---------------------------------end of script for silver.crm_cust_info ----------------------------------------
	---------------------------------start script for silver.crm_prd_info-------------------------------------------
	SET @start_time = GETDATE()
		
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>> Inserting Data Into: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info
	(
		prd_id,
		cat_id, 
		prd_key ,
		prd_nm ,
		prd_cost ,
		prd_line ,
		prd_start_dt,
		prd_end_dt
	)

	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') cat_id,
		REPLACE(SUBSTRING(prd_key, 7 , LEN(prd_key)),'-','_') prd_key,
		prd_nm,
		ISNULL(prd_cost,0) prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
	FROM bronze.crm_prd_info

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	-- table Update on crm_prd_info in the Store procedure was here but I have removed it:
	-- Purpose: It keeps on refreshing/clearing data  the silver layer for crm_prd_info anytime the SP is executed

  	---------------------------------end of script for silver.crm_prd_info ----------------------------------------
	---------------------------------start script for silver.crm_sales_details-------------------------------------
	
  
	-- table Update on silver.crm_sales_details in the Store procedure was here but I have removed it:
	-- Purpose: It keeps on refreshing/clearing data  the silver layer for silver.crm_sales_details anytime the SP is executed

-- an update on silver.crm_sales_details on the prd_key column... Where I replace the '-' with '_'. 

	SET @start_time = GETDATE()
		
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num,
		sls_prd_key,
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
		REPLACE(sls_prd_key, '-','_'), -- Updated the keys: since it was '-' before and has changed it to '_'
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <0 OR LEN(sls_ship_dt) != 8 THEN  NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_ship_dt <0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL OR sls_sales <= 0
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE 
			WHEN  sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_details

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	---------------------------------end of script for silver.crm_sales_details--------------------------------------
	PRINT '--------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------';
	---------------------------------start of script for silver.erp_cust_az12----------------------------------------
	SET @start_time = GETDATE()
	
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12
	(
		cid, 
		bdate, 
		gen
	)
	--Identify out of range bdate
	SELECT 
		CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
		END AS cid,
		-- we have to deal with bad date... bdate > today are not good
		CASE 
			WHEN bdate > GETDATE () THEN NULL 
			ELSE bdate 
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'N/A'
		END gen 
	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	---------------------------------end of script for silver.erp_cust_az12-----------------------------------------
	---------------------------------start of script for silver.erp_loc_a101----------------------------------------
	SET @start_time = GETDATE()
	
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 
	(
		cid, 
		cntry
	)
	SELECT 
	REPLACE(cid, '-','') cid,
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ( 'US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	---------------------------------end of script for silver.erp_loc_a101----------------------------------------
	---------------------------------start of script for silver.erp_px_cat_g1v2-----------------------------------
	SET @start_time = GETDATE()
	
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2 ;

	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(
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
	FROM bronze.erp_px_cat_g1v2
	
	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	---------------------------------end of script for silver.erp_px_cat_g1v2------------------------------------

	SET @batch_end_time = GETDATE ()
		PRINT '=================================================';
		PRINT '	Loading Silver Layer is Completed';
		PRINT '	 - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=================================================';
	
	END TRY
	BEGIN CATCH
	PRINT '======================================================='
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '======================================================='
	END CATCH
END 
