/*
Create a new database named 'data_warehouse_project1' 
and set up three schemas: bronze, silver, and gold.
*/

Use master;
GO

-- Create a new database named 'data_warehouse_project1'.
Create Database data_warehouse_project1;
GO

-- Switch to the new database.
Use data_warehouse_project1;
GO

-- Set up three schemas: bronze, silver, and gold.
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO