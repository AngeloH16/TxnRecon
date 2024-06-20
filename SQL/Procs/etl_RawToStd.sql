CREATE OR REPLACE PROCEDURE etl.rawtostd()
LANGUAGE sql
AS $$
    insert into etl.stg_txns (  
                TransactionDate
                ,Narration              
                ,Debit          
                ,Credit         
                ,Balance        
                ,SourceFile
                ,Source)

    select   
             cast(TransactionDate as date) as TransactionDate
            ,NULLIF(Narration,'') as Narration
            ,cast(NULLIF(Debit,'')as decimal(19,2)) as Debit
            ,cast(NULLIF(Credit,'')as decimal(19,2)) as Credit
            ,cast(NULLIF(Balance,'')as decimal(19,2)) as Balance
            ,NULLIF(sourcefile,'') as SourceFile
            ,NULLIF(source,'') as source
    from etl.raw_txns
    ;


update etl.stg_txns as t 
set filerank = r.rank
from ( select *, ROW_NUMBER () over (partition by TransactionDate
                ,Narration             
                ,Debit          
                ,Credit         
                ,Balance        
                order by sourcefile desc) as rank
                from etl.stg_txns) as r  
        where r.stg_id = t.stg_id

;        

    delete from  etl.stg_txns
    where FileRank > 1
    ;


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
                                on              COALESCE(stg.TransactionDate,'31-12-2100') = COALESCE(std.TransactionDate,'31-12-2100')
                                                and COALESCE(stg.Narration      ,'') = COALESCE(std.Narration      ,'')
                                                and COALESCE(stg.Debit          ,0) = COALESCE(std.Debit          ,0)
                                                and COALESCE(stg.Credit         ,0) = COALESCE(std.Credit         ,0)
                                                and COALESCE(stg.Balance        ,0) = COALESCE(std.Balance        ,0)
                        where std.ID is null) x
        

        ;

    insert into etl.std_txns (  
                TransactionDate
                ,Narration           
                ,Debit          
                ,Credit         
                ,Balance        
                ,load_dt
                ,SourceFile
                ,Source)

    select       TransactionDate
                ,Narration             
                ,Debit          
                ,Credit         
                ,Balance        
                ,now() as load_dt
                ,SourceFile
                ,Source from (select stg.* from etl.stg_txns stg
                        full join etl.std_txns std 
                        on              COALESCE(stg.TransactionDate,'31-12-2100') = COALESCE(std.TransactionDate,'31-12-2100')
                                        and COALESCE(stg.Narration      ,'') = COALESCE(std.Narration      ,'')
                                        and COALESCE(stg.Debit          ,0) = COALESCE(std.Debit          ,0)
                                        and COALESCE(stg.Credit         ,0) = COALESCE(std.Credit         ,0)
                                        and COALESCE(stg.Balance        ,0) = COALESCE(std.Balance        ,0)
                where std.ID is null) x
                ;
    
update etl.std_txns
        set narration = upper(narration);

update  etl.std_txns
        set debit = debit * -1  
        where debit < '0'
        and source = 'CBA';

truncate table etl.stg_txns;
truncate table etl.raw_txns;

$$;
