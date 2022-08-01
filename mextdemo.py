#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import timedelta
import prometheus_client as prom
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


# In[2]:


server_name = "localhost"
db_name = "mextdb"
user_name = "postgres"
pass_word = "mext"
port = "5432"
connection_string = "postgresql+psycopg2://" + user_name + ":" + \
    pass_word + "@" + server_name + ":" + port + "/" + db_name


# In[3]:


alchemy_engine = create_engine(connection_string)
riskScoreDF = pd.read_sql_query(
    "SELECT * FROM riskscore order by date ASC OFFSET 10000 ROWS FETCH NEXT 1000 ROWS ONLY;", alchemy_engine)


# In[4]:


riskScoreDF.head()


# In[5]:


gauge = prom.Gauge(
    "anomaly",
    "anomaly output ",
    ["asset", "model"]
)


# In[6]:


prom.start_http_server(8000)


# In[7]:


len(riskScoreDF)


# In[8]:


if __name__ == '__main__':
    i = 0
    while True:
        #riskScoreDF["score"] = riskScoreDF["score"].replace(np.nan, 0)
        model = riskScoreDF["model"][i]
        asset_name = riskScoreDF["asset"][i]

        t = riskScoreDF['date'][i+1] - riskScoreDF['date'][i]
        t = t.total_seconds()

        gauge.labels(asset_name, model).set(riskScoreDF["score"][i])

        i = i+1
        time.sleep(t)

        print("i", i, riskScoreDF["score"][i])

        if i == (len(riskScoreDF)-1):
            print(len(riskScoreDF))
            i = 0
