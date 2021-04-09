USE [woodman]

IF OBJECT_ID('dbo.EfCoreTestChild') IS NOT NULL
BEGIN
	DROP TABLE [dbo].[EfCoreTestChild]
END

IF OBJECT_ID('dbo.EfCoreTest') IS NOT NULL
BEGIN
	DROP TABLE [dbo].[EfCoreTest]
END


IF OBJECT_ID('dbo.EfCoreTestNonValueTypeKeys') IS NOT NULL
BEGIN
	DROP TABLE [dbo].EfCoreTestNonValueTypeKeys
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

CREATE TABLE [dbo].[EfCoreTestChild](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[EfCoreTestId] [int] NOT NULL,
	[Name] [varchar](50) NULL,
	[CreatedDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_EfCoreTestChild] PRIMARY KEY CLUSTERED 
(
	[ID] ASC,
	[EfCoreTestId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[EfCoreTestChild]  WITH CHECK ADD  CONSTRAINT [FK_EfCoreTestChild_EfCoreTest1] FOREIGN KEY([EfCoreTestId])
REFERENCES [dbo].[EfCoreTest] ([ID])

ALTER TABLE [dbo].[EfCoreTestChild] CHECK CONSTRAINT [FK_EfCoreTestChild_EfCoreTest1]

CREATE TABLE [dbo].EfCoreTestNonValueTypeKeys(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Tier1Id] VARCHAR(64) NOT NULL,
	[Tier2Id] VARCHAR(64) NOT NULL,
	[IsArchived] BIT NOT NULL,
	[Name] [varchar](50) NULL,
	[CreatedDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_EfCoreTestNonValueTypeKeys] PRIMARY KEY CLUSTERED 
(
	[Tier1Id] ASC,
	[Tier2Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]



