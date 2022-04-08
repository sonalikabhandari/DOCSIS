import pandas as pd
import numpy as np
import time
# import MySQLdb
from sqlalchemy import create_engine,exc, text
from sqlalchemy.pool import NullPool
import datetime as dt

import decimal
# engine = create_engine('mysql+pymysql://p2818726:p2818726@98.7.187.4/02_STAGING', poolclass=NullPool)
pd.set_option('display.max_columns', 500)
engine = create_engine('mysql+pymysql://p2818726:p2818726@98.7.187.4/DOCSIS', poolclass=NullPool)
cur = engine.connect()

# result = cur.execute("select * from DownStream LIMIT 10")
#
# for r in result:
#     print(r)

cur.execute('SET GLOBAL connect_timeout=28800')
cur.execute('SET GLOBAL interactive_timeout=28800')
cur.execute('SET GLOBAL wait_timeout=28800')
# #
# store = pd.HDFStore('/home/sona/Projects/DOCSIS/UpStream.h5')
count = 0
chunk_size=100000
offset = 0
dfs = []
while True:
  sql ="""SELECT summaryDate,HubName,Macdomain,`US Offered Bps`,`D1.X`,D2,`D3.0`,`D3.1`,`total modems`
  ,`D3.1 Pct`,`D3.0 Pct`,`US utilizedbps` FROM UpStream limit %d offset %d""" % (chunk_size,offset)
  dfs.append(pd.read_sql(sql, engine))
  offset += chunk_size
  count+=1
  print("offset value no:"+str(offset))
  print("count is:"+str(count))
  if len(dfs[-1]) < chunk_size:
      break
full_df = pd.concat(dfs)
print("Final count is:"+str(count))
print(len(full_df))


full_df['HubName'] = full_df['HubName'].astype(str)
full_df['Macdomain'] = full_df['Macdomain'].astype(str)

# full_df['D1.X'] = full_df['D1.X'].fillna(0, inplace=True)
# full_df['D2'] = full_df['D2'].fillna(0, inplace=True)
# full_df['D3.0'] = full_df['D3.0'].fillna(0, inplace=True)
# full_df['D3.1'] = full_df['D3.1'].fillna(0, inplace=True)
# full_df['D3.1 Pct'] = full_df['D3.1 Pct'].fillna(0, inplace=True)
# full_df['D3.0 Pct'] = full_df['D3.0 Pct'].fillna(0, inplace=True)
#
# full_df['D1.X'] = full_df['D1.X'].astype(str)
# full_df['D2'] = full_df['D2'].astype(str)
# full_df['D3.0'] = full_df['D3.0'].astype(str)
# full_df['D3.1'] = full_df['D3.1'].astype(str)
# full_df['D3.1 Pct'] = full_df['D3.1 Pct'].astype(str)
# full_df['D3.0 Pct'] = full_df['D3.0 Pct'].astype(str)
#



full_df['Month'] = full_df['summaryDate'].dt.strftime('%b')
full_df['Year'] = full_df['summaryDate'].dt.year

# full_df['D1.X'] = pd.to_numeric(full_df['D1.X'], errors='coerce')
# full_df['D2'] = pd.to_numeric(full_df['D2'], errors='coerce')
# full_df['D3.0'] = pd.to_numeric(full_df['D3.0'], errors='coerce')
# full_df['D3.1'] = pd.to_numeric(full_df['D3.1'], errors='coerce')
# full_df['D3.1 Pct'] = pd.to_numeric(full_df['D3.1 Pct'], errors='coerce')
# full_df['D3.0 Pct'] = pd.to_numeric(full_df['D3.0 Pct'], errors='coerce')

print(full_df.head(100))

full_df = full_df.groupby(['Month','HubName','Macdomain']).agg({'US Offered Bps':'sum','US utilizedbps':'sum',
'D1.X':'max','D2':'max','D3.0':'max','D3.1':'max','total modems':'max','D3.1 Pct':'max','D3.0 Pct':'max',
'Year':lambda x: x.iloc[0]}).reset_index()
print(full_df)
print(len(full_df))

full_df.to_hdf('/home/sona/Projects/DOCSIS/upsteam_aggregated.h5', key='upstream_df', mode='w')

print("I have reached the end!!")
cur.close()
