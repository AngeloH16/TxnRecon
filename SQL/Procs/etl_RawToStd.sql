CREATE OR REPLACE PROCEDURE etl.RawToStd()
LANGUAGE SQL
as $$
    insert into etl.stg_txns (  
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
            ,cast(TransactionDate as date) as TransactionDate
            ,NULLIF(Narration,'') as Narration
            ,cast(NULLIF(Cheque,'') as Money) as Cheque
            ,cast(NULLIF(Debit,'')as Money) as Debit
            ,cast(NULLIF(Credit,'')as Money) as Credit
            ,cast(NULLIF(Balance,'')as Money) as Balance
            ,NULLIF(TransactionType,'') as TransactionType
            ,NULLIF(sourcefile,'') as SourceFile
    from etl.raw_txns
    ;




 -- Clean for duplicates in stg
update etl.stg_txns as t 
set filerank = r.rank
from ( select *, ROW_NUMBER () over (partition by BSBNumber      
                ,AccountNumber  
                ,TransactionDate
                ,Narration      
                ,Cheque         
                ,Debit          
                ,Credit         
                ,Balance        
                ,TransactionType
                order by sourcefile desc) as rank
        --into  temp table RANK_CTE
                from etl.stg_txns) as r  
        where r.stg_id = t.stg_id

;        

    delete from  etl.stg_txns
    where FileRank > 1
    ;


    -- select max date from previously ingested tables
        insert into etl.log(
                CalledProc
                ,AffectedTable  
                ,RecordCount
                ,load_dt
                )
        select   'etl.RawToStd'
                ,'etl.std_txns'
                ,count(0)
                ,now()
        from (select stg.* from etl.stg_txns stg
                                full join etl.std_txns std 
                                on              COALESCE(stg.BSBNumber,'')      = COALESCE(std.BSBNumber      ,'')
                                                and COALESCE(stg.AccountNumber  ,'') = COALESCE(std.AccountNumber  ,'')
                                                and COALESCE(stg.TransactionDate,'31-12-2100') = COALESCE(std.TransactionDate,'31-12-2100')
                                                and COALESCE(stg.Narration      ,'') = COALESCE(std.Narration      ,'')
                                                and COALESCE(stg.Cheque         ,'') = COALESCE(std.Cheque         ,'')
                                                and COALESCE(stg.Debit          ,'') = COALESCE(std.Debit          ,'')
                                                and COALESCE(stg.Credit         ,'') = COALESCE(std.Credit         ,'')
                                                and COALESCE(stg.Balance        ,'') = COALESCE(std.Balance        ,'')
                                                and COALESCE(stg.TransactionType,'') = COALESCE(std.TransactionType,'')
                        where std.ID is null) x
        

        ;

    insert into etl.std_txns (  
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
                ,now() as load_dt
                ,SourceFile from (select stg.* from etl.stg_txns stg
                        full join etl.std_txns std 
                        on              COALESCE(stg.BSBNumber,'')      = COALESCE(std.BSBNumber      ,'')
                                        and COALESCE(stg.AccountNumber  ,'') = COALESCE(std.AccountNumber  ,'')
                                        and COALESCE(stg.TransactionDate,'31-12-2100') = COALESCE(std.TransactionDate,'31-12-2100')
                                        and COALESCE(stg.Narration      ,'') = COALESCE(std.Narration      ,'')
                                        and COALESCE(stg.Cheque         ,'') = COALESCE(std.Cheque         ,'')
                                        and COALESCE(stg.Debit          ,'') = COALESCE(std.Debit          ,'')
                                        and COALESCE(stg.Credit         ,'') = COALESCE(std.Credit         ,'')
                                        and COALESCE(stg.Balance        ,'') = COALESCE(std.Balance        ,'')
                                        and COALESCE(stg.TransactionType,'') = COALESCE(std.TransactionType,'')
                where std.ID is null) x
                ;
    


    truncate table etl.stg_txns;
    truncate table  etl.raw_txns;
    --COMMIT;

$$;