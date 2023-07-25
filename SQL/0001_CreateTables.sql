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