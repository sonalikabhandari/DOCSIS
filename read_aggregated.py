import pandas as pd
import numpy as np
import time
# import MySQLdb
from sqlalchemy import create_engine,exc, text
from sqlalchemy.pool import NullPool
import datetime as dt
import decimal


# result = pd.read_hdf('/home/sona/Projects/DOCSIS/downsteam_aggregated.h5', key='final_df')
result = pd.read_hdf('/home/sona/Projects/DOCSIS/upsteam_aggregated.h5', key='upstream_df')

print(len(result))
print(result)
