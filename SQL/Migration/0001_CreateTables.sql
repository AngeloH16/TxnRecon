--USE [TxnRecon]

DROP TABLE IF EXISTS etl.raw_txns
;
CREATE TABLE etl.raw_txns (
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
    ,Source             varchar(5000)
    );
DROP TABLE IF EXISTS etl.stg_txns;
CREATE TABLE etl.stg_txns (
     STG_ID             SERIAL PRIMARY KEY
    ,TransactionDate    date
    ,Narration          varchar(5000)
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,SourceFile         varchar(5000)
    ,Source             varchar(5000)
    ,FileRank           int
    );

DROP TABLE IF EXISTS etl.std_txns;
CREATE TABLE etl.std_txns (
     ID                 SERIAL PRIMARY KEY
    ,TransactionDate    date
    ,Narration          varchar(5000)
    ,Debit              money
    ,Credit             money
    ,Balance            money
    ,load_dt            timestamp 
    ,SourceFile         varchar(5000)
    ,Source             varchar(5000)
    );

DROP TABLE IF EXISTS etl.log;
CREATE TABLE etl.log (
     ID                 SERIAL PRIMARY KEY
    ,CalledProc          varchar(5000)
    ,AffectedTable      varchar(5000)
    ,RecordCount    bigint
    ,load_dt          timestamp
    )
    ;
    COMMIT;