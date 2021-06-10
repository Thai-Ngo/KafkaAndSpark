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

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456",
  database="stock"
)
mycursor = mydb.cursor()

preDate = date(2020, 2, 15)
mycursor.execute(sql_selectStockInfo)
stockList = mycursor.fetchall()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
while 1:
    for stock in stockList:
        mycursor.execute(sql_selectLatestDate, [stock[0]])
        latestDate = mycursor.fetchall()
        if latestDate:
            if latestDate[0][0] > preDate:
                mycursor.execute(sql_selectLastRec, [stock[0], cf.time_steps])
                lastRec = mycursor.fetchall()
                mycursor.execute(sql_checkModelReady, [stock[0]])
                isReady = mycursor.fetchall()
                if isReady[0][0]:  
                    preDate = latestDate[0][0]
                    for rec in lastRec:
                        data = f'{rec[0]} {rec[1]} {rec[2]} {rec[3]} {rec[4]} {rec[5]} {rec[6]}'
                        producer.send('predict', value=data.encode("utf-8"))
    time.sleep(1)