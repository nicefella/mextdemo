import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import timedelta
import prometheus_client as prom
import time
from prometheus_client import CollectorRegistry, Gauge

server_name = "localhost"
db_name = "mextdb"
user_name = "postgres"
pass_word = "mext"
port = "5432"
connection_string = "postgresql+psycopg2://" + user_name + ":" + pass_word + "@" + server_name + ":" + port + "/" + db_name

alchemy_engine = create_engine(connection_string)

print("Veri tabanına bağlandı")
totalData = pd.read_sql_query("SELECT * FROM anomaly order by date ASC;",
                                alchemy_engine)

print("total data alındı")
totalData["score"].replace(to_replace=[None], value=np.nan, inplace=True)

gauge = prom.Gauge(
            "anomaly",
           "anomaly output ", 
            ["asset","model"]
)

prom.start_http_server(5055)

start = 7000 #index start value in dataframe
k=20 #increment value
sonVeri = False

while True :
    if start > (len(totalData)-k):
        k = len(totalData) - (start)
        sonVeri = True
    
    anomalyDF = pd.read_sql_query("SELECT * FROM anomaly order by date ASC OFFSET "
                                        +str(start)+" ROWS FETCH NEXT "+str(k)+" ROWS ONLY;",
                                   alchemy_engine)
    anomalyDF["score"].replace(to_replace=[None], value=np.nan, inplace=True)
    print("Anomaly data çekildi")
    i=0
    while True:        

        model = anomalyDF["model"][i]
        asset_name = anomalyDF["asset"][i]
        
        if i+1 > len(anomalyDF)-1:
            break

        t = anomalyDF['date'][i+1] - anomalyDF['date'][i]
        t = t.total_seconds()

        gauge.labels(asset_name,model).set(anomalyDF["score"][i])

        i=i+1
        time.sleep(t)
        if i == k:
            break

    if sonVeri == False :
        print(k ," adet data yazıldı")
        start = start +k
    else:
        print(start, " veri başarılı bir şekilde basıldı")
        start = 0
        k=10
        sonVeri= False
        