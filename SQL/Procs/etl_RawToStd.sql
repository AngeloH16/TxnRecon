CREATE OR ALTER PROC etl.RawToStd as 
insert into std_txns (  
            BSBNumber      
            ,AccountNumber  
            ,TransactionDate
            ,Narration      
            ,Cheque         
            ,Debit          
            ,Credit         
            ,Balance        
            ,TransactionType)

select   NULLIF(BSBNumber,'') as BSBNumber
        ,NULLIF(AccountNumber,'') as AccountNumber
        ,Convert(date,TransactionDate,103) as TransactionDate
        ,NULLIF(Narration,'') as Narration
        ,cast(NULLIF(Cheque,'') as Money) as Cheque
        ,cast(NULLIF(Debit,'')as Money) as Debit
        ,cast(NULLIF(Credit,'')as Money) as Credit
        ,cast(NULLIF(Balance,'')as Money) as Balance
        ,NULLIF(TransactionType,'') as TransactionType
 from raw_txns

 truncate table raw_txns