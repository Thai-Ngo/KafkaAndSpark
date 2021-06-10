import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from json import loads
import mysql.connector
import sys
import pandas as pd

sql_checkRecExist = "SELECT * FROM stockrecord WHERE stock_name = %s && rec_date = %s"
sql_checkStockExist = "SELECT * FROM stockinfo WHERE stock_name = %s"
sql_insertStockInfo = "INSERT INTO stockinfo(stock_name) VALUE(%s)"
sql_inserStockRec = "INSERT INTO stockrecord VALUES (%s, %s, %s, %s, %s, %s, %s)"


print("Main")
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456",
  database="stock"
)
mycursor = mydb.cursor()

bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'stock-rec'

# Initialize consumer variable
consumer = KafkaConsumer (topicName, group_id ='group3',bootstrap_servers =
  bootstrap_servers, auto_offset_reset='latest',
    enable_auto_commit=False)


for msg in consumer:
    print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
    stock_rec = msg.value.decode("utf-8")
    properties = stock_rec.split(" ")
    try:
        stock_name = properties[0]
        rec_date = properties[1]
        mycursor.execute(sql_checkRecExist, [stock_name, rec_date])
        isRecExist = mycursor.fetchall()
        if not isRecExist:
            print("Rec not exist")
            mycursor.execute(sql_checkStockExist, [stock_name])
            isStockExist = mycursor.fetchall()
            if not isStockExist:
                print("Stock info not exist")
                mycursor.execute(sql_insertStockInfo, [stock_name])
            mycursor.execute(sql_inserStockRec, properties)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
    except:
        e = sys.exc_info()[0]
        print("Error: %s"%e)