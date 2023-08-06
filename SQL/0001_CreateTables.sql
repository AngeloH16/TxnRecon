--USE [TxnRecon]

DROP TABLE IF EXISTS raw_txns
;
CREATE TABLE raw_txns (
     BSBNumber          varchar(5000)
    ,AccountNumber      varchar(5000)
    ,TransactionDate    varchar(5000)
    ,Narration          varchar(5000)
    ,Cheque             varchar(5000)
    ,Debit              varchar(5000)
    ,Credit             varchar(5000)
    ,Balance            varchar(5000)
    ,TransactionType    varchar(5000)
    ,SourceFile         varchar(5000)
    );
DROP TABLE IF EXISTS stg_txns;
CREATE TABLE stg_txns (
     STG_ID             SERIAL PRIMARY KEY
    ,BSBNumber          varchar(5000)
    ,AccountNumber      varchar(5000)
    ,TransactionDate    date
    ,Narration          varchar(5000)
    ,Cheque             money
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,TransactionType    varchar(5000)
    ,SourceFile         varchar(5000)
    ,FileRank           int
    );

DROP TABLE IF EXISTS std_txns;
CREATE TABLE std_txns (
     ID                 SERIAL PRIMARY KEY
    ,BSBNumber          varchar(5000)
    ,AccountNumber      varchar(5000)
    ,TransactionDate    date
    ,Narration          varchar(5000)
    ,Cheque             money
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,TransactionType    varchar(5000)
    ,load_dt            timestamp 
    ,SourceFile         varchar(5000)
    );
GO