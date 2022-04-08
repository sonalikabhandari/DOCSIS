import pandas as pd
import numpy as np
import time
# import MySQLdb
from sqlalchemy import create_engine,exc, text
from sqlalchemy.pool import NullPool
import datetime as dt
import decimal
# import pandasql as ps
# engine = create_engine('mysql+pymysql://p2818726:p2818726@98.7.187.4/02_STAGING', poolclass=NullPool)

# engine = create_engine('mysql+pymysql://p2818726:p2818726@98.7.187.4/DOCSIS', poolclass=NullPool)
# cur = engine.connect()

# result = cur.execute("select * from DownStream LIMIT 10")
#
# for r in result:
#     print(r)



# df_DownStream = pd.read_hdf('/home/sona/Projects/DOCSIS/downsteam.h5','full_df')
# print(len(df_DownStream))

#
# cur.execute('SET GLOBAL connect_timeout=28800')
# cur.execute('SET GLOBAL interactive_timeout=28800')
# cur.execute('SET GLOBAL wait_timeout=28800')
# #
store = pd.HDFStore('/home/sona/Projects/DOCSIS/downsteam.h5')
# count = 0
# chunk_size=100000
# offset = 0
# dfs = []
# while True:
#   sql = "SELECT * FROM DownStream limit %d offset %d" % (chunk_size,offset)
#   dfs.append(pd.read_sql(sql, engine))
#   offset += chunk_size
#   count+=1
#   print("offset value no:"+str(offset))
#   print("count is:"+str(count))
#   if len(dfs[-1]) < chunk_size:
#       break
# full_df = pd.concat(dfs)
# print("Final count is:"+str(count))
# print(len(full_df))
#
# full_df['Legacy_Company'] = full_df['Legacy_Company'].astype(str)
# full_df['Region_Name'] = full_df['Region_Name'].astype(str)
# full_df['MgtArea'] = full_df['MgtArea'].astype(str)
# full_df['DMA'] = full_df['DMA'].astype(str)
# full_df['HubName'] = full_df['HubName'].astype(str)
# full_df['Macdomain'] = full_df['Macdomain'].astype(str)
# full_df['SG'] = full_df['SG'].astype(str)
#
# full_df['D1.X'] = full_df['D1.X'].fillna(0, inplace=True)
# full_df['D2'] = full_df['D2'].fillna(0, inplace=True)
# full_df['D3.0'] = full_df['D3.0'].fillna(0, inplace=True)
# full_df['D3.1'] = full_df['D3.1'].fillna(0, inplace=True)
#
# full_df['D1.X'] = full_df['D1.X'].astype(str)
# full_df['D2'] = full_df['D2'].astype(str)
# full_df['D3.0'] = full_df['D3.0'].astype(str)
# full_df['D3.1'] = full_df['D3.1'].astype(str)
#
# dfs = np.array_split(full_df, 52)
# for i in dfs:
#     store.append('name_of_frame', i, format='t', append = True,  data_columns=True, min_itemsize={'DMA':50,'HubName':50,'Macdomain':50,'SG':50},index=False)

# print(len(store.select("name_of_frame",where='summaryDate ="2020-05-24 00:00:00"')))

# print(store.select("name_of_frame",where='summaryDate ="2020-05-24 00:00:00"'))
pd.set_option('display.max_columns', 500)
df_Jan = store.select("name_of_frame",where='summaryDate >="2020-01-01" & summaryDate <="2020-05-31"')
# df = store.select("name_of_frame")
df_Jan['Month'] = df_Jan['summaryDate'].dt.strftime('%b')
df_Jan['Year'] = df_Jan['summaryDate'].dt.year

# df['D1.X'] = df['D1.X'].fillna(0, inplace=True)
# df['D2'] = df['D2'].fillna(0, inplace=True)
#
# df['D3.0'] = df['D3.0'].fillna(0, inplace=True)
#
# df['D3.1'] = df['D3.1'].fillna(0, inplace=True)

# for i ,r in df.iterrows():
#     if r['D1.X'] is None:
#         r['D1.X'] = 0
# print(df['D2'].dtypes)
#
# print(df['D1.X'].dtypes)
#
# print(df['D3.0'].dtypes)
# print(df['D3.1'].dtypes)
#
# df['D1.X'] = df['D1.X'].astype(str).astype(int)
# df['D2'] = df['D2'].astype(str).astype(int)
# df['D3.0'] = df['D3.0'].astype(str).astype(int)
# df['D3.1'] = df['D3.1'].astype(str).astype(int)
# print("after")
#
# print(df['D2'].dtypes)
#
# print(df['D1.X'].dtypes)
#
# print(df['D3.0'].dtypes)
# print(df['D3.1'].dtypes)

df_Jan['D1.X'] = pd.to_numeric(df_Jan['D1.X'], errors='coerce')
df_Jan['D2'] = pd.to_numeric(df_Jan['D2'], errors='coerce')
df_Jan['D3.0'] = pd.to_numeric(df_Jan['D3.0'], errors='coerce')
df_Jan['D3.1'] = pd.to_numeric(df_Jan['D3.1'], errors='coerce')
print(df_Jan['D2'].dtypes)

print(df_Jan['D1.X'].dtypes)

print(df_Jan['D3.0'].dtypes)
print(df_Jan['D3.1'].dtypes)

print(df_Jan['Total Modems'].dtypes)

df_Jan = df_Jan.groupby(['Month','HubName','Macdomain']).agg({'DS offeredbps':'sum','DS utilizedbps':'sum',
'D1.X':'max','D2':'max','D3.0':'max','D3.1':'max','Total Modems':'max',
'Year':lambda x: x.iloc[0],'Legacy_Company':lambda x: x.iloc[0],'Region_Name':lambda x: x.iloc[0],
'MgtArea':lambda x: x.iloc[0],'DMA':lambda x: x.iloc[0],'SG':lambda x: x.iloc[0],}).reset_index()
print(df_Jan)
print(len(df_Jan))

df_feb = store.select("name_of_frame",where='summaryDate >="2020-06-01" & summaryDate <="2020-09-30"')
# df = store.select("name_of_frame")
df_feb['Month'] = df_feb['summaryDate'].dt.strftime('%b')
df_feb['Year'] = df_feb['summaryDate'].dt.year


df_feb['D1.X'] = pd.to_numeric(df_feb['D1.X'], errors='coerce')
df_feb['D2'] = pd.to_numeric(df_feb['D2'], errors='coerce')
df_feb['D3.0'] = pd.to_numeric(df_feb['D3.0'], errors='coerce')
df_feb['D3.1'] = pd.to_numeric(df_feb['D3.1'], errors='coerce')


df_feb = df_feb.groupby(['Month','HubName','Macdomain']).agg({'DS offeredbps':'sum','DS utilizedbps':'sum',
'D1.X':'max','D2':'max','D3.0':'max','D3.1':'max','Total Modems':'max',
'Year':lambda x: x.iloc[0],'Legacy_Company':lambda x: x.iloc[0],'Region_Name':lambda x: x.iloc[0],
'MgtArea':lambda x: x.iloc[0],'DMA':lambda x: x.iloc[0],'SG':lambda x: x.iloc[0],}).reset_index()
print(df_feb)
print(len(df_feb))


store.close()

df_final = df_Jan.append(df_feb)
print(df_final)
print(len(df_final))

df_final.to_hdf('/home/sona/Projects/DOCSIS/downsteam_aggregated.h5', key='final_df', mode='w')


# full_df.to_hdf('/home/sona/Projects/DOCSIS/downsteam.h5', key='full_df', mode='w')
# print(pd.read_hdf("/home/sona/Projects/DOCSIS/downsteam.h5", "full_df", start=0, stop=2))

# full_df['Legacy_Company'] = full_df['Legacy_Company'].astype()

# result = pd.read_hdf('/home/sona/Projects/DOCSIS/data.h5', 'full_df')
# print(len(result))
# print(result)

# count = 0
# chunk_size=10000
# offset = 0
# dfs = []
# while True:
#   sql = "SELECT * FROM DownStream limit %d offset %d" % (chunk_size,offset)
#   dfs.append(pd.read_sql(sql, engine))
#   offset += chunk_size
#   count+=1
#   print("offset value no:"+str(offset))
#   print("count is:"+str(count))
#   if len(dfs[-1]) < chunk_size:
#     break
# full_df = pd.concat(dfs)
# print("Final count is:"+str(count))
# print(len(full_df))



# query_upStream = "select * from UpStream"

# start_time = time.time()
#
# df_downStream = pd.read_sql(query_downStream, engine, chunksize=50000)
#
# count = 0
#
# def chunks_to_df(gen):
#     count+=1
#     chunks = []
#     for df in gen:
#         print(len(df))
#         print(count)
#         chunks.append(df)
#     return pd.concat(chunks).reset_index().drop('index', axis=1)
#
# result_downStream = chunks_to_df(df_downStream)
#
# print("--- %s seconds ---" % (time.time() - start_time))
# cur.close()
