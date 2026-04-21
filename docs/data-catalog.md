# Data Catalog for Gold Layer
> **Layer:** Gold  
> **Schema:** `gold`  
> **Purpose:** Business-ready data modeled as a Star Schema for analytics and BI reporting 

## Overview
The Gold layer is the final, analytics-optimized layer of the Medallion Architecture. Data here has been fully cleaned (Silver), integrated from both CRM and ERP source systems, and restructured into a **Star Schema** consisting of:
- **1 Fact Table** — transactional business events
- **3 Dimension Tables** — descriptive context for analytics

---
 
## Table Reference

### 1. `gold.dim_customers`

**Description:** Customer dimension table. Integrates customer profile data from the CRM system with additional data from the ERP system.
**Source Tables:** `silver.crm_cust_info`, `silver.erp_cust_az12`, `silver.erp_loc_a101`

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key     | INT           | Surrogate key uniquely identifying each customer record in the dimension table.               |
| customer_id      | INT           | Unique numerical identifier assigned to each customer.                                        |
| customer_number  | NVARCHAR(50)  | Alphanumeric identifier representing the customer.        |
| first_name       | NVARCHAR(50)  | The customer's first name.                                         |
| last_name        | NVARCHAR(50)  | The customer's last name.                                                      |
| gender           | NVARCHAR(50)  | The gender of the customer (e.g., `Male`, `Female`, `Unknown`)                                |
| country          | NVARCHAR(50)  | The country of residence for the customer.                                |
| marital_status   | NVARCHAR(50)  | The marital status of the customer (e.g., `Married`, `Single`).                               |
| birthdate        | DATE          | The date of birth of the customer.                |
| create_date      | DATE          | The date and time when the customer record was created in the system                          |

---

### 2. `gold.dim_products`

**Description:** Product dimension table. Combines product master data from CRM with category and subcategory data from the ERP system.
**Source Tables:** `silver.crm_prd_info`, `silver.erp_px_cat_g1v2`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
|product_key|INT|Surrogate key uniquely identifying each product record in the dimension table.|
|product_id|INT| Unique numerical identifier assigned to each product. |
|product_number|NVARCHAR(50)|Alphanumeric identifier representing the product. |
|product_name|NVARCHAR(50)| Full product name.|
|category_id|NVARCHAR(50)| A unique identifier for the product's category.|
|category|NVARCHAR(50)| Top-level product category (e.g., `Bikes`, `Accessories`)
|subcategory|NVARCHAR(50)| Product subcategory (e.g., `Road Bikes`, `Jerseys`)|
|maintenance|NVARCHAR(50)| Maintenance flag: `Yes` or `No`|
|cost|INT| Standard cost of the product |
|product_line|NVARCHAR(50)| Product line classification (e.g., `Touring`, `Mountain`)
|start_date|DATE| Date the product became available. |

---

### 3. `gold.fact_sales`

**Description:** Central fact table for sales transactions. Each row represents one order line item. Links to all dimension tables via surrogate keys.
**Source Tables:** `silver.crm_sales_details`, plus surrogate key lookups from `dim_customers` and `dim_products`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
|order_number| NVARCHAR(50) | A unique alphanumeric identifier for each sales order|
|product_key|INT| Surrogate key linking the order to the `gold.dim_products.product_key`|
|customer_key|INT| Surrogate key linking the order to the `gold.dim_customers.customer_key`|
|order_date|DATE|Date the order was placed|
|shipping_date|DATE|Date the order was shipped|
|due_date|DATE|Expected delivery date|
|sales_amoung|INT|Total revenue for the line item|
|quantity|INT|Number of units ordered|
|price|INT|Unit price at time of sale|
