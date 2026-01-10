
/*
============================================================================
DDL Script: Create Gold Views
============================================================================

Script Purpose:
  This script creates views for the Gold layer in the data warehouse.
  The Gold layer represents the final dimension and fact tables (Star Schema)
  
  Each view performs transformations and combines data from the Silver layer
  to produce a clean, enriched, and business-ready dataset.

Usage:
  - These views can be queried directly for analytics and reporting.
============================================================================

*/


/*
=========================================================
Object Name : gold.dim_customers
Layer       : Gold
Object Type : Dimension (View)
Purpose     : Stores enriched customer master data
              with demographic and geographic attributes.
Grain       : One row per customer (current state)
=========================================================
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate Key
    ci.cst_id        AS customer_id,     -- Business Key
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender enrichment logic (CRM is master source)
    CASE 
        WHEN ci.cst_gndr <> 'N/A' THEN TRIM(ci.cst_gndr)
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,

    ca.bdate          AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

/*
=========================================================
Object Name : gold.dim_products
Layer       : Gold
Object Type : Dimension (View)
Purpose     : Stores current product master data.
Grain       : One row per active product.
=========================================================
*/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- Surrogate Key
    pi.prd_id   AS product_id,       -- Business Key
    pi.prd_key  AS product_number,
    pi.cat_id   AS category_id,
    pi.prd_nm   AS product_name,
    pc.cat      AS category,
    pc.subcat   AS sub_category,
    pc.maintenance,
    pi.prd_line AS product_line,
    pi.prd_cost AS product_cost,
    pi.prd_start_dt AS start_date

FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- Active products only
GO

/*
=========================================================
Object Name : gold.fact_sales
Layer       : Gold
Object Type : Fact (View)
Purpose     : Stores transactional sales metrics.
Grain       : One row per order per product per customer.
=========================================================
*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num   AS order_number,
    ci.customer_key,
    pr.product_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers ci
    ON sd.sls_cust_id = ci.customer_id
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number;
GO
