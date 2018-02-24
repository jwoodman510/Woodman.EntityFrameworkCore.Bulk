USE [woodman]

IF OBJECT_ID('dbo.EfCoreTest') IS NOT NULL
BEGIN
	DROP TABLE [dbo].[EfCoreTest]
END

CREATE TABLE [dbo].[EfCoreTest](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](50) NULL,
	[CreatedDate] datetime not null,
	[ModifiedDate] datetime not null
	CONSTRAINT [PK_EfCoreTest] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]


IF OBJECT_ID('dbo.Split') IS NOT NULL
BEGIN
	DROP FUNCTION [dbo].[Split]
END