import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import timedelta
import prometheus_client as prom
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
from sklearn.preprocessing import LabelEncoder
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

server_name = "localhost"
db_name = "mextdb"
user_name = "postgres"
pass_word = "mext"
port = "5432"
connection_string = "postgresql+psycopg2://" + user_name + ":" + pass_word + "@" + server_name + ":" + port + "/" + db_name

alchemy_engine = create_engine(connection_string)
anomalyDF = pd.read_sql_query("SELECT * FROM anomaly order by date ASC;"
                                , alchemy_engine)

gauge = prom.Gauge(
            "anomaly",
           "anomaly output ", 
            ["asset","model"]
)

prom.start_http_server(5055)

if __name__ == '__main__':
    i=0
    while True:
        model = anomalyDF["model"][i]
        asset_name = anomalyDF["asset"][i]
        
        t = anomalyDF['date'][i+1] - anomalyDF['date'][i]
        t = t.total_seconds()
        
        gauge.labels(asset_name,model).set(anomalyDF["score"][i])
               
        i=i+1
        time.sleep(t)
        
        #print("i",i,riskScoreDF["score"][i]) 
        
        if i == (len(anomalyDF)-1):
            print(len(anomalyDF))
            i=0
    
    