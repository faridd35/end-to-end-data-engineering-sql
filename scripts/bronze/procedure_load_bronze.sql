create or alter procedure bronze.load_bronze as
BEGIN
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY
		set @batch_start_time = GETDATE();
		print '==========================';
		print 'Loading Bronze Layer';
		print '==========================';

		print '--------------------------';
		print 'Loading CRM Tables';
		print '--------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print '>> Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SELECT count(*) from bronze.crm_cust_info;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		print '>> Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		select count(*) from bronze.crm_prd_info;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		print '>> Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		select count(*) from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		print '--------------------------';
		print 'Loading ERP Tables';
		print '--------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		print '>> Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		select COUNT(*) from bronze.erp_cust_az12;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set	@start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		print '>> Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		select COUNT(*) from bronze.erp_loc_a101;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		print '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Muhammad Farid\Documents\GitHub\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		select COUNT(*) from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		print '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '--------------------------';

		set @batch_end_time = GETDATE();
		print '==========================';
		print 'Finished Loading Bronze Layer';
		print 'Total Load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '==========================';
	END TRY
	
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END