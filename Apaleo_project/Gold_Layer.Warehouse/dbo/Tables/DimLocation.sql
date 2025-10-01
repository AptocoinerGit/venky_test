CREATE TABLE [dbo].[DimLocation] (

	[LocationKey] int NULL, 
	[LocationCountryCode] varchar(50) NOT NULL, 
	[LocationCity] varchar(100) NULL, 
	[LocationAddress] varchar(250) NULL, 
	[LocationPostalCode] varchar(250) NULL, 
	[InsertDate] datetime2(3) NULL, 
	[UpdateDate] datetime2(3) NULL
);