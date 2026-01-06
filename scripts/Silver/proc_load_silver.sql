/*
==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze" schema.

Actions Performed:
  - Truncates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None
  This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
Script to execute the store procedure to Load data into Silver Layer 
==============================================================================
*/
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

	PRINT '>> Inserting Data Into: silver.crm_cut_info';
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
		TRIM (cst_firstname) AS cst_firstname,
		TrIM (cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER (TRIM (cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER (TRIM (cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
		CASE
			WHEN UPPER (TRIM (cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER (TRIM (cst_gndr)) = 'F' THEN 'Female'
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

	----------------------------------------end-----------------------------------------------------------------------
	----------------------------------------start---------------------------------------------------------------------
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
		REPLACE (SUBSTRING (prd_key, 1, 5),'-','_') cat_id,
		REPLACE (SUBSTRING (prd_key, 7 , LEN(prd_key)),'-','_') prd_key,
		prd_nm,
		ISNULL (prd_cost,0) prd_cost,
		CASE 
			WHEN UPPER (TRIM (prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER (TRIM (prd_line)) = 'R' THEN 'Road'
			WHEN UPPER (TRIM (prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER (TRIM (prd_line)) = 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
	FROM bronze.crm_prd_info

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	----Updating Table: silver.crm_prd_info after data cleaning
	IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

	CREATE TABLE silver.crm_prd_info 
	(
		prd_id INT,
		cat_id NVARCHAR (50), -- Add this created column
		prd_key NVARCHAR (50),
		prd_nm NVARCHAR (50),
		prd_cost INT,
		prd_line NVARCHAR (50),
		prd_start_dt DATE, -- data type change from DATETIME to DATE
		prd_end_dt DATE,  -- data type change from DATETIME to DATE
		dwh_create_date DATETIME2 DEFAULT GETDATE ()
	);
  ---------------------------------end script for silver.crm_prd_info -------------------------------------
	---------------------------------start script for silver.crm_sales_details-------------------------------
	
  -- Updating Table: silver.crm_sales_details after data cleaning

	IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

	CREATE TABLE silver.crm_sales_details (
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id INT,
		sls_order_dt DATE,
		sls_ship_dt DATE,
		sls_due_dt DATE,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DATETIME2 DEFAULT GETDATE ()
	);

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
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <0 OR LEN (sls_order_dt) != 8 THEN NULL
			ELSE CAST (CAST (sls_order_dt AS VARCHAR ) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <0 OR LEN (sls_ship_dt) != 8 THEN  NULL
			ELSE CAST (CAST (sls_ship_dt AS VARCHAR ) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_ship_dt <0 OR LEN (sls_ship_dt) != 8 THEN NULL
			ELSE CAST (CAST (sls_ship_dt AS VARCHAR ) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales != sls_quantity * ABS (sls_price) OR sls_sales IS NULL OR sls_sales <= 0
			THEN sls_quantity * ABS (sls_price)
			ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE 
			WHEN  sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF (sls_quantity,0)
			ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_details

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	----------------------------------------end-----------------------------------------------------------------------
	PRINT '--------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------';
	----------------------------------------start---------------------------------------------------------------------
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
		WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid)) 
		ELSE cid 
		END AS cid,
		-- we have to deal with bad date... bdate > today are not good
		CASE 
			WHEN bdate > GETDATE () THEN NULL 
			ELSE bdate 
		END AS bdate,
		CASE 
			WHEN UPPER (TRIM (gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER (TRIM (gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'N/A'
		END gen 
	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	----------------------------------------end-----------------------------------------------------------------------
	----------------------------------------start---------------------------------------------------------------------
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
	REPLACE (cid, '-','') cid,
		CASE 
			WHEN TRIM (cntry) = 'DE' THEN 'Germany'
			WHEN TRIM (cntry) IN ( 'US', 'USA') THEN 'United States'
			WHEN TRIM (cntry) IS NULL OR cntry = '' THEN 'N/A'
			ELSE TRIM (cntry)
		END AS cntry
	FROM bronze.erp_loc_a101

	SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	----------------------------------------end-----------------------------------------------------------------------
	----------------------------------------start---------------------------------------------------------------------
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
