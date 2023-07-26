USE [TxnRecon]
IF OBJECT_ID('raw_txns', N'U') IS NOT NULL
DROP TABLE raw_txns
GO
CREATE TABLE raw_txns (
     BSBNumber          nvarchar(max)
    ,AccountNumber      nvarchar(max)
    ,TransactionDate    nvarchar(max)
    ,Narration          nvarchar(max)
    ,Cheque             nvarchar(max)
    ,Debit              nvarchar(max)
    ,Credit             nvarchar(max)
    ,Balance            nvarchar(max)
    ,TransactionType    nvarchar(max)
    ,SourceFile         nvarchar(max)
    );
GO
IF OBJECT_ID('stg_txns', N'U') IS NOT NULL
DROP TABLE stg_txns
GO
CREATE TABLE stg_txns (
     STG_ID                 int identity(1,1) PRIMARY KEY
    ,BSBNumber          nvarchar(max)
    ,AccountNumber      nvarchar(max)
    ,TransactionDate    date
    ,Narration          nvarchar(max)
    ,Cheque             money
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,TransactionType    nvarchar(max)
    ,SourceFile         nvarchar(max)
    ,FileRank           int
    );
GO
IF OBJECT_ID('std_txns', N'U') IS NOT NULL
DROP TABLE std_txns
GO
CREATE TABLE std_txns (
     ID                 int identity(1,1) PRIMARY KEY
    ,BSBNumber          nvarchar(max)
    ,AccountNumber      nvarchar(max)
    ,TransactionDate    date
    ,Narration          nvarchar(max)
    ,Cheque             money
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,TransactionType    nvarchar(max)
    ,load_dt            datetime
    ,SourceFile         nvarchar(max)
    );
GO