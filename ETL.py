#!/usr/bin/env python
# coding: utf-8

# In[15]:


# CSV Data Ingestion

import pandas as pd
import glob
import shutil
from SQLFunctions import *
import os
from dotenv import load_dotenv
load_dotenv(override=True)
import psycopg2 
from psycopg2 import Error
import psycopg2.extras as extras
import numpy as np
from pathlib import Path
from tkinter import filedialog


# Concat all CSVs in data file to one df

# In[16]:


directory = filedialog.askdirectory()
files = glob.glob(directory+"/*.csv")


# In[17]:


df_raw_data = pd.DataFrame()


for csv in files:
    frame = pd.read_csv(csv)
    frame['SourceFile'] = os.path.basename(csv)
    df_raw_data = pd.concat([df_raw_data,frame])
    


# Clean column names for whitespace

# In[18]:


df_raw_data.columns = [c.replace(' ', '') for c in df_raw_data.columns]


# Replace Nulls

# In[19]:


df_raw_data_clean = df_raw_data.replace(np.nan, '', regex=True)


# Anonymise data

# In[20]:


df_raw_data_clean["AccountNumber"] = '************'+df_raw_data_clean["AccountNumber"].str[-4:]


# In[21]:


df_raw_data_clean["SourceFile"] = df_raw_data_clean["SourceFile"].str[-26:]


# ## Move files to DB

# Test Connect to DB

# In[22]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))
connection_test(connection)


# Funciton for insert into raw

# Insert into Raw

# In[23]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

insert_values(connection,df_raw_data_clean,'etl.raw_txns')


# Call ETL proc

# In[25]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

execute_proc(connection,'etl.RawToStd()')



# Read in logging

# In[26]:


query = 'select recordcount,AffectedTable,calledproc from  etl.log l inner join (select max(id) as max_id from etl.log) m on l.id = m.max_id'
column_names = ['recordcount','AffectedTable','calledproc']


# In[27]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))


log = postgresql_to_dataframe(connection, query, column_names)


# In[28]:


print(log.recordcount.map(str)+' rows inserted into '+log.AffectedTable.map(str)+' by '+log.calledproc.map(str))


# Convert to py script

# In[29]:


get_ipython().system('jupyter nbconvert --to script ETL.ipynb')

