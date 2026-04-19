/* 
=======================================
Quality Check Queries for Gold Layer
========================================
Purpose: To identify validate of data integrity, data consistency, and data accuracy of the Gold Layer. 
These queries check for Uniqueness of surrogate keys in dimension tables, referencial integrity between fact
and dimension tables, and validation of relationships in the data model for analytical purposes.
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
select
    customer_key,
    count(*) duplicate_count
from gold.dim_customers
group by customer_key
having count(*) > 1;

select * from gold.dim_products;

-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
select
    product_key,
    count(*) duplicate_count
from gold.dim_products
group by product_key
having count(*) > 1;

select * from gold.dim_products;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
select * 
from gold.fact_sales fs
left join gold.dim_customers dc
on fs.customer_key = dc.customer_key
left join gold.dim_products dp
on fs.product_key = dp.product_key
where dp.product_key is null or dc.customer_key is null;

select * from gold.fact_sales;