/*
=======================================
Quality Check Queries for Silver Layer
========================================
Purpose: To identify data quality issues in the Silver layer tables before loading them into the Gold layer. 
These queries check for duplicates, null values, inconsistent formatting, and logical errors in the data.

Usage: Run these queries against the Silver layer tables to identify any data quality issues that need to be 
addressed before promoting the data to the Gold layer.

*/

-- =====================================
-- Checking: silver.crm_cust_info
-- =====================================

-- Check for duplicates and nulls in primary key
select cst_id, count(*) 
from silver.crm_cust_info 
group by cst_id 
having count(*) > 1 or cst_id is null;

-- Check for leading/trailing spaces in key and name fields
select cst_key 
from silver.crm_cust_info
where cst_key != trim(cst_key);

select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

-- Data standardization & consistency 
select distinct cst_gndr from silver.crm_cust_info;

select distinct cst_marital_status from silver.crm_cust_info;

-- =====================================
-- Checking: silver.crm_prd_info
-- =====================================

-- Check for duplicates and nulls in primary key
select prd_id, count(*) 
from silver.crm_prd_info
group by prd_id 
having count(*) > 1 or prd_id is null;

-- Check for leading/trailing spaces in key and name fields
select prd_key from silver.crm_prd_info
where prd_key != trim(prd_key);

select prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for negative or null costs
select prd_cost from silver.crm_prd_info
where prd_cost < 0or prd_cost is null;

-- Data standardization & consistency
select distinct prd_line from silver.crm_prd_info;

-- Logical consistency: start date should be before end date
select * from silver.crm_prd_info
where prd_start_dt > prd_end_dt;

-- =====================================
-- Checking: silver.crm_sales_details
-- =====================================

-- Check for relational integrity: sls_cust_id should exist in silver.crm_cust_info
select * from silver.crm_sales_details 
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Check for invalid dates in sls_order_dt, sls_ship_dt, sls_due_dt
select NULLIF(sls_due_dt,0) sls_due_dt 
from bronze.crm_sales_details 
where sls_due_dt <= 0 
or len(sls_due_dt) != 8
or sls_due_dt > 20450101
or sls_due_dt < 19450101;

select * from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt 
or sls_order_dt > sls_due_dt 
or sls_ship_dt > sls_due_dt;

-- Check for logical consistency in sales, quantity, and price
select sls_sales, sls_quantity, sls_price from silver.crm_sales_details
where sls_sales <=0 or sls_sales is null 
or sls_sales != sls_quantity * abs(sls_price)
or sls_quantity <= 0
or sls_price <=0 or sls_price is null
or sls_price != sls_sales / sls_quantity;


-- =====================================
-- Checking: silver.erp_cust_az12
-- =====================================

-- Check for relational integrity: cid should exist in silver.crm_cust_info
select cid from silver.erp_cust_az12 
where cid not in (select cst_key from silver.crm_cust_info);

-- Check for invalid dates in bdate
select bdate from silver.erp_cust_az12 
where bdate > getdate() or bdate < dateadd(year, -120, getdate());

-- Data standardization & consistency
select distinct gen from silver.erp_cust_az12;

-- =====================================
-- Checking: silver.erp_loc_a101
-- =====================================

-- Check for relational integrity: cid should exist in silver.crm_cust_info
select cid from silver.erp_loc_a101 
where cid not in (select cst_key from silver.crm_cust_info);

-- Data standardization & consistency
select distinct cntry from silver.erp_loc_a101;

-- =====================================
-- Checking: silver.erp_px_cat_g1v2
-- =====================================

-- Check for relational integrity: id should exist in silver.crm_prd_info
select id from silver.erp_px_cat_g1v2 
where id not in (select cat_id from silver.crm_prd_info);

-- Check for leading/trailing spaces in key and name fields
select * from silver.erp_px_cat_g1v2
where cat is null or cat != trim(cat)
or subcat is null or subcat != trim(subcat)
or maintenance is null or maintenance != trim(maintenance);

-- Data standardization & consistency
select distinct cat from silver.erp_px_cat_g1v2;

select distinct subcat from silver.erp_px_cat_g1v2;

select distinct maintenance from silver.erp_px_cat_g1v2;