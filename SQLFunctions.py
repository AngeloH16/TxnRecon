import os
import psycopg2 
from psycopg2 import Error
import psycopg2.extras as extras
import pandas as pd


def connection_test(connection):
    try:
        # Connect to an existing database
        conn = connection

        # Create a cursor to perform database operations
        cursor = conn.cursor()
        # # Print PostgreSQL details
        # print("PostgreSQL server information")
        # print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("Testing connection to - ", connection, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()
            # print("PostgreSQL connection is closed")


def insert_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    # print("the dataframe is inserted")
    cursor.close()

def execute_proc(conn, procname): 
    call = 'call '+procname
    try: 
        cursor = conn.cursor()
  
        # call stored procedure 
        cursor.execute(call)
        conn.commit()
        cursor.close()  
        conn.close()  
        #print('Executed ',procname,' Successfully')
        
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error while connecting to PostgreSQL", error) 
  
    finally: 
        
        # closing database connection. 
        if conn: 
            cursor.close()  
            conn.close()
            #print("PostgreSQL connection is closed") 

def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()
    
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df