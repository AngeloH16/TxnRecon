#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2 
from psycopg2 import Error
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(override=True)


# Test connect to DB

# In[2]:


try:
    # Connect to an existing database
    connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # # Print PostgreSQL details
    # print("PostgreSQL server information")
    # print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("Testing connection to - ", record, "\n")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


# Funciton for loading SQL into dataframem

# In[4]:


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


# In[5]:


query = 'select  id,bsbnumber,accountnumber,transactiondate,narration,cheque,debit,credit,balance,transactiontype,load_dt,sourcefile from etl.std_txns LIMIT 1000 '
column_names = ['id','bsbnumber','accountnumber','transactiondate','narration','cheque','debit','credit','balance','transactiontype','load_dt','sourcefilefrom']


# In[6]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))


df = postgresql_to_dataframe(connection, query, column_names)


# In[7]:


df


# Convert to py script

# In[ ]:


get_ipython().system('jupyter nbconvert --to script TextML.ipynb')

