/*
DDL Script: Creating views in the Gold layer of the data warehouse. 
These views are designed to provide a clean and structured representation of the data for reporting and analysis purposes. 
The views include a dimension for customers, a dimension for products, and a fact table for sales. 
*/


-- ============================================================
-- Create Dimension: gold.dim_customers
-- ============================================================
if object_id('gold.dim_customers', 'V') is not null
    drop view gold.dim_customers;
GO

create view gold.dim_customers as
select 
	row_number() over (order by ci.cst_id) as customer_key, --surrogate key
	ci.cst_id customer_id, 
	ci.cst_key customer_number, 
	ci.cst_firstname first_name, 
	ci.cst_lastname last_name, 
	case when ci.cst_gndr != 'Unknown' then ci.cst_gndr
		else coalesce(ca.gen, 'Unknown')
	end as gender,
	la.cntry country,
	ci.cst_marital_status marital_status, 
	ca.bdate birthdate,
	ci.cst_create_date create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la on ci.cst_key = la.cid;
GO

-- ============================================================
-- Create Dimension: gold.dim_products
-- ============================================================
if object_id('gold.dim_products', 'V') is not null
    drop view gold.dim_products;
GO

create view gold.dim_products as
select
	row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key, --surrogate key
	pn.prd_id product_id,
	pn.prd_key product_number,
	pn.prd_nm product_name,
	pn.cat_id category_id,
	pc.cat category,
	pc.subcat subcategory,
	pc.maintenance,
	pn.prd_cost cost,
	pn.prd_line product_line,
	pn.prd_start_dt start_date
from silver.crm_prd_info pn
left join silver.erP_px_cat_g1v2 pc on pn.cat_id = pc.id
where pn.prd_end_dt is null;  --filtering out the products which are not active anymore
GO

-- ============================================================
-- Create Fact Table: gold.fact_sales
-- ============================================================
if object_id('gold.fact_sales', 'V') is not null
    drop view gold.fact_sales;
GO

create view gold.fact_sales as
select 
	sd.sls_ord_num order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt order_date,
	sd.sls_ship_dt shipping_date,
	sd.sls_due_dt due_date,
	sd.sls_sales sales_amount,
	sd.sls_quantity quantity,
	sd.sls_price price
from silver.crm_sales_details sd
left join gold.dim_customers dc on sd.sls_cust_id = dc.customer_id
left join gold.dim_products dp on sd.sls_prd_key = dp.product_number;
GO
