/*
==============================================================================
DDL: Create Silver Layer Tables
==============================================================================
Purpose:
  Defines the canonical schema for the Silver layer.
  Silver tables store cleansed, standardized, and integration-ready data
  sourced from the Bronze layer.

Design Notes:
  - Business keys are standardized to NVARCHAR to enable cross-source joins.
  - Dates are stored using DATE data types.
  - Tables are reload-safe and designed for full refresh ETL patterns.
============================================================================== 
*/

------------------------------------------------------------
-- CRM CUSTOMER INFO
------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    cst_id NVARCHAR(50),         -- Business customer identifier
    cst_key NVARCHAR(50),        -- Alternate customer key
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

------------------------------------------------------------
-- CRM PRODUCT INFO
------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
(
    prd_id NVARCHAR(50),         -- Business product identifier
    cat_id NVARCHAR(50),         -- Product category ID
    prd_key NVARCHAR(50),        -- Product business key
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

------------------------------------------------------------
-- CRM SALES DETAILS
------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num NVARCHAR(50),    -- Order number
    sls_prd_key NVARCHAR(50),    -- Product business key
    sls_cust_id NVARCHAR(50),    -- Customer business ID
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

------------------------------------------------------------
-- ERP CUSTOMER DEMOGRAPHICS
------------------------------------------------------------
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

------------------------------------------------------------
-- ERP CUSTOMER LOCATION
------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

------------------------------------------------------------
-- ERP PRODUCT CATEGORY
------------------------------------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
