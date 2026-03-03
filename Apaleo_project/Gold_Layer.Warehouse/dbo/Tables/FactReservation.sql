CREATE TABLE [dbo].[FactReservation] (

	[FactReservationKey] bigint NOT NULL, 
	[PropertiesKey] int NOT NULL, 
	[ArrivalDate] date NULL, 
	[CreatedDate] datetime2(3) NULL, 
	[CheckInTime] datetime2(3) NULL, 
	[CheckOutTime] datetime2(3) NULL, 
	[DepartureDate] date NULL, 
	[Status] varchar(50) NULL, 
	[TotalGrossAmount] decimal(18,2) NULL, 
	[InsertDate] datetime2(3) NULL, 
	[UpdateDate] datetime2(3) NULL
);