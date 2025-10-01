CREATE TABLE [dbo].[DimProperties] (

	[PropertiesKey] int NULL, 
	[LocationKey] int NULL, 
	[Code] varchar(50) NOT NULL, 
	[Id] varchar(50) NOT NULL, 
	[Name] varchar(200) NULL, 
	[CompanyName] varchar(200) NULL, 
	[InsertDate] datetime2(3) NULL, 
	[UpdateDate] datetime2(3) NULL
);