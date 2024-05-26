#!/usr/bin/env python
# coding: utf-8

# In[27]:


import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
from SQLFunctions import *


# Test Connection

# In[29]:


connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))
connection_test(connection)


# Loop over procs folder

# In[39]:


folder_path = 'SQL/Procs'

sqlFile = ''
for filename in os.listdir(folder_path):
    if filename.endswith('.sql'):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r') as file:
            # Do something with the file
            sqlFile = file.read()
            clean_sqlfile = sqlFile.replace('\n', '  ')
            connection = psycopg2.connect(user=os.getenv('POSTGRES_USER'),
                                  password=os.getenv('POSTGRES_PASSWORD'),
                                  host=os.getenv('POSTGRES_HOST'),
                                  port=os.getenv('POSTGRES_PORT'),
                                  database=os.getenv('POSTGRES_DATABASE'))

            cursor = connection.cursor()
            cursor.execute(clean_sqlfile)
            print(f"Deployed {filename} successfully to {connection.info.dbname}")
            cursor.close()
            connection.close()


# Convert to py script

# In[ ]:


get_ipython().system('jupyter nbconvert --to script sql_run.ipynb')

