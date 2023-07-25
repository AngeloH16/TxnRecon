use master
GO 

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'TxnRecon')
  BEGIN
    CREATE DATABASE [TxnRecon]


    END
    GO
       USE [TxnRecon]
    GO