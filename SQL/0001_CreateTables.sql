USE [TxnRecon]
IF OBJECT_ID('raw_txns', N'U') IS NULL
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
    );
GO

IF OBJECT_ID('std_txns', N'U') IS NULL
CREATE TABLE std_txns (
     BSBNumber          nvarchar(max)
    ,AccountNumber      nvarchar(max)
    ,TransactionDate    date
    ,Narration          nvarchar(max)
    ,Cheque             money
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,TransactionType    nvarchar(max)
    );
GO