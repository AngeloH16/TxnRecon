CREATE OR REPLACE PROCEDURE etl.testproc()
 LANGUAGE SQL
 as $$;
     select * from etl.log;
 $$;
