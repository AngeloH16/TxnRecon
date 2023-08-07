# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: DataScience
#     language: python
#     name: python3
# ---

# %%
# CSV Data Ingestion

import pandas as pd
import glob2
import shutil
import os
from dotenv import load_dotenv
load_dotenv(override=True)
import pyodbc 
import numpy as np

# %% [markdown]
# Concat all CSVs in data file to one df

# %%
path = 'Data'
csv_files = glob.glob(path+"/*.csv")

df_raw_data = pd.DataFrame()

for csv in csv_files:
    frame = pd.read_csv(csv)
    frame['SourceFile'] = os.path.basename(csv)
    df_raw_data = pd.concat([df_raw_data,frame])
    


# %% [markdown]
# Move files to archive

# %%
source_dir = 'Data'
target_dir = 'Data/Old'
    
file_names = os.listdir(source_dir)
    
for file_name in file_names:
    shutil.move(os.path.join(source_dir, file_name), target_dir)

# %% [markdown]
# Update file name to remove PII

# %%
# Extract the datge of the file 
df_raw_data['SourceFileDate'] = df_raw_data['SourceFile'].str[32:]

# Match the card number to the BW card (from .env)
BW = os.getenv('BANKWEST_LAST4')
df_raw_data['SourceFile'] = df_raw_data['SourceFile'].str[15:19].apply(lambda x: 'BANKWEST' if x == BW else 'UNKNOWN')

# Concat date and card tag
df_raw_data['SourceFile'] = df_raw_data['SourceFile'].astype(str)+'_'+df_raw_data['SourceFileDate'].astype(str)

# Remove sourcefile column
df_raw_data.drop('SourceFileDate', axis = 1, inplace=True)

# %% [markdown]
# Hash PII

# %%
df_raw_data['Account Number'] = df_raw_data['Account Number'].apply(hash)
df_raw_data['BSB Number'] = df_raw_data['BSB Number'].apply(hash)

# %% [markdown]
# Clean column names for whitespace

# %%
df_raw_data.columns = [c.replace(' ', '') for c in df_raw_data.columns]

# %% [markdown]
# Replace Nulls

# %%
df_raw_data_clean = df_raw_data.replace(np.nan, '', regex=True)

# %% [markdown]
# Move files to DB

# %%

server = os.getenv('SQL_SERVER')
database = os.getenv('SQL_DATABASE')
cnxn_str = 'DRIVER={SQL Server};server='+server+';Database='+database+';Trusted_Connection=yes;'
cnxn = pyodbc.connect(cnxn_str)
cursor = cnxn.cursor()


# %%
for index, row in df_raw_data_clean.iterrows():
    cursor.execute("Insert into raw_txns (BSBNumber,AccountNumber,TransactionDate,Narration,Cheque,Debit,Credit,Balance,TransactionType,SourceFile) Values (?,?,?,?,?,?,?,?,?,?)", row.BSBNumber,row.AccountNumber,row.TransactionDate,row.Narration,row.Cheque,row.Debit,row.Credit,row.Balance,row.TransactionType,row.SourceFile)
cnxn.commit()

# %%
cursor.execute("EXEC etl.RawToStd")
cnxn.commit()
