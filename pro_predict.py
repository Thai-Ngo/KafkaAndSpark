import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from json import loads
import mysql.connector
import sys
import pandas as pd
from datetime import date, datetime, timedelta
import time
from kafka import KafkaProducer

import train_model_config as cf

sql_selectLatestDate = "SELECT MAX(rec_date) FROM stockrecord WHERE stock_name = %s"
sql_selectLastRec = "SELECT * FROM stockrecord WHERE stock_name = %s ORDER BY rec_date desc limit %s"
sql_selectStockInfo = "SELECT * FROM stockinfo"
sql_checkModelReady = "SELECT ready FROM stockinfo WHERE stock_name = %s"


print("Pro predict")

mydb = mysql.connector.connect(
  host=cf.host,
  user=cf.user,
  password=cf.password,
  database=cf.database
)
mycursor = mydb.cursor()

preDate = {}

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while 1:
    mycursor.execute(sql_selectStockInfo)
    stockList = mycursor.fetchall()
    
    for stock in stockList:
        if stock[0] not in preDate:
            preDate[stock[0]] = date(1970, 2, 15)
        mycursor.execute(sql_selectLatestDate, [stock[0]])
        latestDate = mycursor.fetchall()
        if latestDate:
            if latestDate[0][0] > preDate[stock[0]]:
                mycursor.execute(sql_selectLastRec, [stock[0], cf.time_steps])
                lastRec = mycursor.fetchall()
                mycursor.execute(sql_checkModelReady, [stock[0]])
                isReady = mycursor.fetchall()
                if isReady[0][0]:
                    preDate[stock[0]] = latestDate[0][0]
                    print("Produce predict: ",stock[0], " ", preDate[stock[0]])
                    for rec in lastRec:
                        data = f'{rec[0]} {rec[1]} {rec[2]} {rec[3]} {rec[4]} {rec[5]} {rec[6]}'
                        producer.send('predict', value=data.encode("utf-8"))
    mydb.commit()
    time.sleep(1)