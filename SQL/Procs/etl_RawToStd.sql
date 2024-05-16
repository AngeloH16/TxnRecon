CREATE OR REPLACE PROCEDURE etl.RawToStd()
LANGUAGE SQL
as $$
Begin
    insert into stg_txns (  
                BSBNumber      
                ,AccountNumber  
                ,TransactionDate
                ,Narration      
                ,Cheque         
                ,Debit          
                ,Credit         
                ,Balance        
                ,TransactionType
                ,SourceFile)

    select   NULLIF(BSBNumber,'') as BSBNumber
            ,NULLIF(AccountNumber,'') as AccountNumber
            ,Convert(date,TransactionDate,103) as TransactionDate
            ,NULLIF(Narration,'') as Narration
            ,cast(NULLIF(Cheque,'') as Money) as Cheque
            ,cast(NULLIF(Debit,'')as Money) as Debit
            ,cast(NULLIF(Credit,'')as Money) as Credit
            ,cast(NULLIF(Balance,'')as Money) as Balance
            ,NULLIF(TransactionType,'') as TransactionType
            ,NULLIF(sourcefile,'') as SourceFile
    from raw_txns;

    -- Clean for duplicates in stg
    with RANK_CTE as (select *, ROW_NUMBER () over (partition by BSBNumber      
                ,AccountNumber  
                ,TransactionDate
                ,Narration      
                ,Cheque         
                ,Debit          
                ,Credit         
                ,Balance        
                ,TransactionType
                order by sourcefile desc) as rank
                from stg_txns)
    Update c 
    set FileRank = c.rank
    from RANK_CTE c;

    delete from  stg_txns
    where FileRank > 1
    ;

    ;

    -- select max date from previously ingested tables
    DROP TABLE IF EXISTS  #TxnInsert

    select * into #TxnInsert
    from (
    select stg.* from stg_txns stg
    full join std_txns std 
    on              ISNULL(stg.BSBNumber,'')      = ISNULL(std.BSBNumber      ,'')
                and ISNULL(stg.AccountNumber  ,'') = ISNULL(std.AccountNumber  ,'')
                and ISNULL(stg.TransactionDate,'') = ISNULL(std.TransactionDate,'')
                and ISNULL(stg.Narration      ,'') = ISNULL(std.Narration      ,'')
                and ISNULL(stg.Cheque         ,'') = ISNULL(std.Cheque         ,'')
                and ISNULL(stg.Debit          ,'') = ISNULL(std.Debit          ,'')
                and ISNULL(stg.Credit         ,'') = ISNULL(std.Credit         ,'')
                and ISNULL(stg.Balance        ,'') = ISNULL(std.Balance        ,'')
                and ISNULL(stg.TransactionType,'') = ISNULL(std.TransactionType,'')
    where std.ID is null
    ) as x

    ;

    insert into std_txns (  
                BSBNumber      
                ,AccountNumber  
                ,TransactionDate
                ,Narration      
                ,Cheque         
                ,Debit          
                ,Credit         
                ,Balance        
                ,TransactionType
                ,load_dt
                ,SourceFile)

    select          BSBNumber
                    ,AccountNumber  
                ,TransactionDate
                ,Narration      
                ,Cheque         
                ,Debit          
                ,Credit         
                ,Balance        
                ,TransactionType
                ,getdate() as load_dt
                ,SourceFile from #TxnInsert;

    DROP TABLE IF EXISTS  #TxnInsert;
    truncate table stg_txns;
    truncate table raw_txns;
    COMMIT
end;$$