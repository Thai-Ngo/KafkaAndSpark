import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from json import loads
import mysql.connector
import sys
import pandas as pd
import math
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
from kafka import KafkaProducer
from datetime import date

import train_model_config as cf

sql_selectStockRec = f'SELECT r.* FROM stockrecord as r, stockinfo as i WHERE r.stock_name = %s and i.stock_name = r.stock_name and r.rec_date > i.last_model order by r.rec_date desc limit {cf.train_size}'
sql_selectStockInfo = "SELECT * FROM stockinfo"
sql_updateLastModel = "UPDATE stockinfo SET last_model = %s, ready = 0 WHERE stock_name = %s"

print("Pro train data")

mydb = mysql.connector.connect(
  host=cf.host,
  user=cf.user,
  password=cf.password,
  database=cf.database
)
mycursor = mydb.cursor()
cur = mydb.cursor( buffered=False , dictionary=True)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


while 1:
    mycursor.execute(sql_selectStockInfo)
    stockList = mycursor.fetchall()
    for stock in stockList:
        mycursor.execute(sql_selectStockRec, [stock[0]])
        rec = mycursor.fetchall()
        if rec:
            if len(rec) == cf.train_size:
                print(stock[0], " Produce train data")
                mycursor.execute(sql_updateLastModel, [rec[0][1], stock[0]])
                mydb.commit()
                for row in rec:
                    try:
                        data = f'{row[0]} {row[1]} {row[2]} {row[3]} {row[4]} {row[5]} {row[6]}'
                        producer.send('train', value=data.encode("utf-8"))
                    except:
                        e = sys.exc_info()[0]
                        print("Error: %s"%e)
    mydb.commit()
    time.sleep(2)