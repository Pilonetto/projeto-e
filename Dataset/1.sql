USE MASTER 
GO
DECLARE @COUNT INTEGER

SET @COUNT = NULL
IF (SELECT  COUNT(DBID)  FROM SYSDATABASES WHERE NAME = 'Analysis') = 0 
BEGIN
  CREATE DATABASE Analysis
END  

GO
IF (SELECT  COUNT(DBID)  FROM SYSDATABASES WHERE NAME = 'Analysis') > 0 
  USE Analysis
GO

IF NOT EXISTS(select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Clients')
BEGIN
	CREATE TABLE Clients (
		ExCompanyId INTEGER,
		Client VARCHAR(100),
		Recency INTEGER,
		RecencyCluster SMALLINT,
		Frequency INTEGER,
		FrequencyCluster SMALLINT,
		Total REAL,
		TotalCluster SMALLINT,
		OverallScore SMALLINT,
		Segment VARCHAR(25),
		RecencyText VARCHAR(25),
		FrequencyText VARCHAR(25),
		TotalText VARCHAR(25)
	)
END
GO

IF NOT EXISTS(select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Sales')
BEGIN
	CREATE TABLE Sales (
		ExCompanyId INTEGER,
		CompanyCode VARCHAR(6),
		OrderNumber INTEGER,
		Employee VARCHAR(100),
		Product VARCHAR(100),
		ProductCategory VARCHAR(100),
		Client VARCHAR(100),
		ClientCity VARCHAR(100),
		SaleDateTime SMALLDATETIME,
		ProductCost REAL,
		DiscountAmount REAL,
		Amount REAL,
		Total REAL, 
		FormPayment VARCHAR(100)
	)
END
GO

IF NOT EXISTS(select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MonthlyMetrics')
BEGIN
	CREATE TABLE MonthlyMetrics (
		ExCompanyId INTEGER,
	)
END

