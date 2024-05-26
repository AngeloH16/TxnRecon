CREATE OR REPLACE PROCEDURE etl.testproc()
LANGUAGE SQL
as $$;
    select now();
$$;