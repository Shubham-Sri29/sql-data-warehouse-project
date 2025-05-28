/*
📘 SP: bronze.load_bronze
Purpose: Loads raw CSV data into staging (bronze) tables for CRM and ERP systems. Acts as the first step in the ETL process.
🔄 Key Steps:
Start Timer – Logs overall load time.
Load CRM Tables – Truncates & bulk loads:
crm_cust_info ← cust_info.csv
crm_prd_info ← prd_info.csv
crm_sales_details ← sales_details.csv
Load ERP Tables – Truncates & bulk loads:
erp_cust_az12 ← CUST_AZ12.csv
erp_loc_a101 ← LOC_A101.csv
erp_px_cat_g1v2 ← PX_CAT_G1V2.csv
Error Handling – Catches and prints error details.
End Timer – Logs total duration.
⚠️ Notes:
File paths are hardcoded.
Assumes CSV format is valid.
No transformations — raw data load only.
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_overall DATETIME , @end_time_overall DATETIME
	BEGIN TRY
		SET @start_time_overall = GETDATE();
		PRINT'================================================';
		PRINT'Loading Bronze Layer';
		PRINT'================================================';

		PRINT'------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT'TRUNCATING TABLE: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'INSERTING DATA INTO: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';
		PRINT'TRUNCATING TABLE: bronze.crm_prd_info';

		SET @start_time = GETDATE();


		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'INSERTING DATA INTO: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

		PRINT'TRUNCATING TABLE: bronze.crm_sales_details';

		SET @end_time = GETDATE();

		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'INSERTING DATA INTO: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

		PRINT'------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'------------------------------------------------';

		PRINT'TRUNCATING TABLE: bronze.erp_cust_az12';

		SET @start_time = GETDATE();

		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'INSERTING DATA INTO: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

		PRINT'TRUNCATING TABLE: bronze.erp_loc_a101';

		SET @start_time = GETDATE();

		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'INSERTING DATA INTO: bronze.erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

		PRINT'TRUNCATING TABLE: bronze.erp_loc_a101';

		SET @start_time = GETDATE();

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'INSERTING DATA INTO: bronze.erp_loc_a101';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\pgshu\Desktop\Shubham_Srivastava\Prompts\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

		SET @end_time_overall = GETDATE();
		PRINT'--------------------------------------------------------';
		PRINT'OVERALL LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_time_overall, @end_time_overall) AS NVARCHAR) + ' seconds.';
		PRINT'--------------------------------------------------------';

	END TRY
	BEGIN CATCH
		PRINT'======================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'======================================================';
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'======================================================';
	END CATCH
END
