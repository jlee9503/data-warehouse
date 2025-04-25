/*
Create and initialize the database and schemas

Purpose:
  - This script initializes the database for the Data Warehouse project.
  - It creates a new database named DataWarehouse and sets the necessary options.
  - It creates the required schemas: bronze, silver, and gold.

Warning:
  - This script will drop the existing DataWarehouse database if it exists.
  - Ensure that you have backups of any important data before running this script.
*/

USE master;
GO

-- Check if the database already exists and drop it if it does
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    DROP DATABASE DataWarehouse;
END
GO
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- Create the necessary schemas for the Data Warehouse
CREATE SCHEMA bronze;
GO
GRANT CONTROL ON SCHEMA::bronze TO sa;
GO
CREATE SCHEMA silver;
GO
GRANT CONTROL ON SCHEMA::silver TO sa;
GO
CREATE SCHEMA gold;
GO
GRANT CONTROL ON SCHEMA::gold TO sa;
GO