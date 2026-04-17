/*
Load Silver Layer (Bronze Layer -> Silver Layer)

This stored procedure performs the ETL (Extract, Transform, and Load) process to populate the 'silver' schema tables from the 'bronze' schema.
Actions:
- Truncate Silver tables
- Inserts transformed and cleansed data from Bronze into Silver tables
- Show Load Duration Messages for each extract tables and batch procedure

Usage Procedure in new query:
	EXEC silver.load_silver;
*/

create or alter procedure silver.load_silver as
begin
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY
		set @batch_start_time = getdate();
		print '==========================';
		print 'Loading Silver Layer';
		print '==========================';

		print '--------------------------';
		print 'Loading CRM Tables';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		print '>> Inserting Data into Table: silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				else 'Unknown'
			end as cst_marital_status,
			case
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'Unknown'
			end as cst_gndr,
			cst_create_date
		from (
			select *, row_number() over (partition by cst_id order by cst_create_date desc) as rn
			from bronze.crm_cust_info
			where cst_id is not null
		) as sub
		where rn = 1;
		select @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		print '>> Inserting Data into Table: silver.crm_prd_info';
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
			substring(prd_key, 7, len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			case upper(trim(prd_line))
				when 'R' then 'Road'
				when 'M' then 'Mountain'
				when 'T' then 'Touring'
				when 'S' then 'Other Sales'
				else 'Unknown'
			end as prd_line,
			prd_start_dt,
			dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		print '>> Inserting Data into Table: silver.crm_sales_details';
		insert into silver.crm_sales_details (
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
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt <= 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case when sls_ship_dt <= 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case when sls_due_dt <= 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case 
				when sls_sales <=0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) 
					then sls_quantity * abs(sls_price)
				else sls_sales
			end as sls_sales, 
			sls_quantity,
			case 
				when sls_price = 0 or sls_price is null
					then sls_sales / NULLIF(sls_quantity,0)
				when sls_price < 0 then abs(sls_price)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details;
		set @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		print '>> Inserting Data into Table: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select
			case when cid like 'NAS%' then substring(cid, 4, len(cid))
				else cid
			end as cid,
			case when bdate > getdate() then null
				else bdate
			end as bdate,
			case upper(trim(gen))
					when 'M' then 'Male'
					when 'MALE' then 'Male'
					when 'F' then 'Female'
					when 'FEMALE' then 'Female'
				else 'Unknown'
			end as gen
		from bronze.erp_cust_az12;
		set @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		print '>> Inserting Data into Table: silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (cid, cntry)
		select 
			replace(cid, '-', '') as cid,
			case 
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US', 'USA', 'United States') then 'United States'
				when trim(cntry) is null or cntry = '' then 'Unknown'
				else trim(cntry)
			end as cntry
		from bronze.erp_loc_a101;
		set @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		print '>> Inserting Data into Table: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
		set @end_time = getdate();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @batch_end_time = getdate();
		print '==========================';
		print 'Finished Loading Silver Layer';
		print 'Total Load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '==========================';
	end try

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end