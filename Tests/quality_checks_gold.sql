/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
  This script performs quality checks to validate the integrity, consastency,
  and accuracy of the gold Layer. These checks ensures
  - Uniqueness of surrogate keys in dimension tabbes.
  - Inferential integrity between fact and dinension tables.
  - Validation of relationships in the data model for analytical purposes.

USAGE NOTES
  - Push these checks after data Loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/

/*
TEST SCRIPTS (Logical / Functional)
tests/test_dim_customers.sql
*/
-- Test: Duplicate customer records
SELECT customer_id, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Test: Gender domain values
SELECT DISTINCT gender
FROM gold.dim_customers;

-- Smoke test
SELECT TOP 100 *
FROM gold.dim_customers;

/*
DATA QUALITY CHECKS (Integrity & Trust)
quality_checks/qc_dim_customers.sql
*/
-- Null surrogate keys should never exist
SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL;

-- Business key completeness
SELECT *
FROM gold.dim_customers
WHERE customer_id IS NULL;

------------------------------------------
/*
TEST SCRIPTS (Logical / Functional)
tests/test_dim_products.sql
*/
-- Test: Duplicate products
SELECT product_id, COUNT(*)
FROM gold.dim_products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Smoke test
SELECT TOP 100 *
FROM gold.dim_products;

/*
DATA QUALITY CHECKS (Integrity & Trust)
quality_checks/qc_dim_products.sql
*/
-- Validate product keys
SELECT *
FROM gold.dim_products
WHERE product_key IS NULL;

-- Ensure active products only
SELECT *
FROM gold.dim_products
WHERE start_date IS NULL;
------------------------------------
/*
TEST SCRIPTS (Logical / Functional)
tests/test_fact_sales.sql
*/
-- Smoke test
SELECT TOP 100 *
FROM gold.fact_sales;

-- Volume check
SELECT COUNT(*) AS row_count
FROM gold.fact_sales;

/*
DATA QUALITY CHECKS (Integrity & Trust)
quality_checks/qc_fact_sales.sql
*/
-- Customer FK integrity
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Product FK integrity
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
----------------------------
