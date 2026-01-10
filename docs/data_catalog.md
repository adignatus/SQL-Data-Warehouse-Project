# Data Dictionary For Gold Layer

## Overview

The Gold Layer is the business-level data representation within the Medallion architecture, structured to support analytical queries and reporting use cases. It consists of conformed dimensions and fact tables that provide business-specific metrics and insights.

**Source:** Transformed and enriched from the Silver Layer (cleaned and validated data)  
**Domain:** Sales and customer analytics  
**Schema Type:** Views (read-only analytical layer)  
**Refresh Cadence:** Daily batch refresh at 2:00 AM UTC  
**Last Updated:** January 10, 2026

---

## 1. gold_dim_customers

**Purpose:** Stores customer master data enriched with demographics and geographic information for analytical segmentation and reporting.

**Technical Details:**
- **Grain:** One row per unique customer
- **Unique Identifier:** customer_key
- **SCD Type:** Type 1 (current state only, overwrites on change)
- **Refresh Strategy:** Full reload daily from Silver Layer

**Columns:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| customer_key | INT | Surrogate key uniquely identifying each customer record. Auto-generated integer for data warehouse use. NOT NULL. |
| customer_id | INT | Unique numerical identifier assigned to each customer in the source system. NOT NULL. |
| customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tracking and referencing in reports and customer-facing systems. NOT NULL. |
| first_name | NVARCHAR(50) | The customer's first name, as recorded in the system. NULLABLE. |
| last_name | NVARCHAR(50) | The customer's last name or family name. NULLABLE. |
| country | NVARCHAR(50) | The country of residence for the customer (e.g., 'Australia', 'United States'). Standardized to ISO country names. NULLABLE. |
| marital_status | NVARCHAR(50) | The marital status of the customer (e.g., 'Married', 'Single'). Constrained values: ['Married', 'Single', 'Unknown']. NULLABLE. |
| gender | NVARCHAR(50) | The gender of the customer (e.g., 'Male', 'Female', 'Non-binary', 'Prefer not to say'). NULLABLE. |
| birthdate | DATE | The date of birth of the customer, formatted as YYYY-MM-DD (e.g., 1971-10-06). Used to calculate age and generational cohorts. NULLABLE. |
| create_date | DATE | The date when the customer record was first created in the source system. Time component not tracked. NOT NULL. |

---

## 2. gold_dim_products

**Purpose:** Provides comprehensive product master data with hierarchical categorization and attributes for product performance analysis and inventory reporting.

**Technical Details:**
- **Grain:** One row per unique product
- **Unique Identifier:** product_key
- **SCD Type:** Type 1 (current state only, overwrites on change)
- **Refresh Strategy:** Full reload daily from Silver Layer

**Columns:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| product_key | INT | Surrogate key uniquely identifying each product record. Auto-generated integer. NOT NULL. |
| product_id | INT | A unique identifier assigned to the product in the source system for internal tracking and referencing. NOT NULL. |
| product_number | NVARCHAR(50) | A structured alphanumeric code representing the product (e.g., SKU), often used for categorization or inventory management. NOT NULL. |
| product_name | NVARCHAR(50) | Descriptive name of the product, including key details such as type, color, and size. NOT NULL. |
| category_id | NVARCHAR(50) | A unique identifier for the product's category, linking to its high-level classification. Used for hierarchical rollups. NULLABLE. |
| category | NVARCHAR(50) | The broader classification of the product (e.g., 'Bikes', 'Components', 'Accessories') to group related items. NULLABLE. |
| subcategory | NVARCHAR(50) | A more detailed classification of the product within the category (e.g., 'Mountain Bikes', 'Road Bikes'). NULLABLE. |
| maintenance_required | NVARCHAR(50) | Indicates whether the product requires ongoing maintenance (e.g., 'Yes', 'No'). Used for service planning. NULLABLE. |
| cost | INT | The cost or base price of the product in whole currency units. Must be >= 0. Used for margin calculations. NULLABLE. |
| product_line | NVARCHAR(50) | The specific product line or series to which the product belongs (e.g., 'Road', 'Mountain', 'Touring'). NULLABLE. |
| start_date | DATE | The date when the product became available for sale or use. Used to filter active products by time period. NULLABLE. |

---

## 3. gold_fact_sales

**Purpose:** Stores transactional sales data at the order line item level for sales performance analysis, revenue tracking, and customer purchasing behavior insights.

**Technical Details:**
- **Grain:** One row per order line item (order + product combination)
- **Related Dimensions:** Links to gold_dim_products (via product_key) and gold_dim_customers (via customer_key)
- **Measure Types:** Additive (sales_amount, quantity, price)
- **Refresh Strategy:** Incremental daily load (new orders only)
- **Historical Range:** Contains sales data from 2010-01-01 to 2014-12-31

**Columns:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| order_number | NVARCHAR(50) | A unique alphanumeric identifier for each sales order (e.g., 'SO54496'). NOT NULL. |
| product_key | INT | Surrogate key linking the order line to the product dimension table (gold_dim_products). NOT NULL. |
| customer_key | INT | Surrogate key linking the order to the customer dimension table (gold_dim_customers). NOT NULL. |
| order_date | DATE | The date when the order was placed by the customer. Primary date dimension for time-series analysis. NOT NULL. |
| shipping_date | DATE | The date when the order was shipped to the customer. Used to calculate fulfillment times. NULLABLE (if not yet shipped). |
| due_date | DATE | The date when the order payment is due or expected delivery date. Used for payment tracking and SLA monitoring. NULLABLE. |
| sales_amount | INT | **[MEASURE]** The total monetary value of the sale for the line item in whole currency units (e.g., 25). Calculated as quantity × price. Must be >= 0. NOT NULL. |
| quantity | INT | **[MEASURE]** The number of units of the product ordered for the line item (e.g., 1). Must be > 0. NOT NULL. |
| price | INT | **[MEASURE]** The price per unit of the product for the line item in whole currency units (e.g., 25). Reflects actual selling price. Must be >= 0. NOT NULL. |
