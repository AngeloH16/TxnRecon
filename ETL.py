#!/usr/bin/env python
# coding: utf-8

# In[6]:


# CSV Data Ingestion

import pandas as pd
import glob
import shutil
import os
from dotenv import load_dotenv
load_dotenv(override=True)
import psycopg2 
from psycopg2 import Error
import psycopg2.extras as extras
import numpy as np


# Concat all CSVs in data file to one df

# In[7]:


path = 'Data'
csv_files = glob.glob(path+"/*.csv")

df_raw_data = pd.DataFrame()

for csv in csv_files:
    frame = pd.read_csv(csv)
    frame['SourceFile'] = os.path.basename(csv)
    df_raw_data = pd.concat([df_raw_data,frame])
    


# Clean column names for whitespace

# In[8]:


df_raw_data.columns = [c.replace(' ', '') for c in df_raw_data.columns]


# Replace Nulls

# In[9]:


df_raw_data_clean = df_raw_data.replace(np.nan, '', regex=True)


# Anonymise data

# In[10]:


df_raw_data_clean["AccountNumber"] = '************'+df_raw_data_clean["AccountNumber"].str[-4:]


# In[11]:


df_raw_data_clean["SourceFile"] = df_raw_data_clean["SourceFile"].str[-26:]


# ## Move files to DB

# Test Connect to DB

# In[12]:


try:
    # Connect to an existing database
    connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


# Funciton for insert into raw

# In[13]:


def execute_values(conn, df, table):
  
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
    print("the dataframe is inserted")
    cursor.close()


# Insert into Raw

# In[14]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

execute_values(connection,df_raw_data_clean,'etl.raw_txns')


# Function for calling ETL proc

# In[15]:


def execute_proc(conn, procname): 
    call = 'call '+procname
    try: 
        cursor = conn.cursor()
  
        # call stored procedure 
        cursor.execute(call)
        connection.commit()
        cursor.close()  
        connection.close()  
        print('Executed ',procname,' Successfully')
        
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error while connecting to PostgreSQL", error) 
  
    finally: 
        
        # closing database connection. 
        if conn: 
            cursor.close()  
            connection.close()
            print("PostgreSQL connection is closed") 
  


# exec proc

# In[16]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

execute_proc(connection,'etl.RawToStd()')



# Move files to archive

# In[17]:


source_dir = 'Data'
target_dir = 'Data/Old'
    
file_names = os.listdir(source_dir)
    
for file_name in file_names:
    shutil.move(os.path.join(source_dir, file_name), target_dir)


# Convert to py script

# In[ ]:


get_ipython().system('jupyter nbconvert --to script ETL.ipynb')


# In[18]:


# server = os.getenv('SQL_SERVER')
# database = os.getenv('SQL_DATABASE')
# cnxn_str = 'DRIVER={SQL Server};server='+server+';Database='+database+';Trusted_Connection=yes;'
# cnxn = pyodbc.connect(cnxn_str)
# cursor = cnxn.cursor()


# In[19]:


# cursor.execute('call etl.RawToStd()')
# cursor.close()  
# connection.close()  


# In[20]:


# for index, row in df_raw_data_clean.iterrows():
#     cursor.execute("Insert into raw_txns (BSBNumber,AccountNumber,TransactionDate,Narration,Cheque,Debit,Credit,Balance,TransactionType,SourceFile) Values (?,?,?,?,?,?,?,?,?,?)", row.BSBNumber,row.AccountNumber,row.TransactionDate,row.Narration,row.Cheque,row.Debit,row.Credit,row.Balance,row.TransactionType,row.SourceFile)
# cnxn.commit()


# In[21]:


# cursor.execut   e("EXEC etl.RawToStd")
# cnxn.commit()


# In[22]:


# procname = 'etl.RawToStd()'
# call = 'call '+procname
# cursor = connection.cursor()
# cursor.execute(call)
# connection.commit()
# cursor.close()  
# connection.close()  

