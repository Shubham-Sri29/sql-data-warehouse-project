/*
======================================================
Create Database and Schemas
======================================================
Script Purpose:
	This script creates a new Database named 'DataWarehouse' after checking if it exists or not.
	If the Database exists, it is dropped and then recreated. Additionally, this script also creates three schemas inside the database named 'bronze', 'silver' & 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' Database if it exists.
	All the data will be deleted from the database permanently, Proceed with caution and take back-up before executing this script if necessary.
*/



USE master;
GO

-- Drop and Recreate the 'DataWarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
