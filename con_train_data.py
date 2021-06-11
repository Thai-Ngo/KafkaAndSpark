import math
import matplotlib.pyplot as plt
import keras
import pandas as pd
import numpy as np
import mysql.connector
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras.layers import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from keras.callbacks import EarlyStopping
from kafka import KafkaConsumer

import train_model_config as cf
sql_trueReady = "UPDATE stockinfo SET ready = 1 WHERE stock_name = %s"

def train(df):
    training_set = df.iloc[0:, 5:6].values
    sc = MinMaxScaler(feature_range = (0, 1))
    training_set_scaled = sc.fit_transform(training_set)
    # Creating a data structure with 10 time-steps and 1 output
    X_train = []
    y_train = []
    for i in range(cf.time_steps, cf.train_size):
        X_train.append(training_set_scaled[i-cf.time_steps:i, 0])
        y_train.append(training_set_scaled[i, 0])
    X_train, y_train = np.array(X_train), np.array(y_train)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    print(X_train.shape)
    model = Sequential()
    model.add(LSTM(128, return_sequences=True, input_shape= (X_train.shape[1], 1)))
    model.add(LSTM(64, return_sequences=False))
    model.add(Dense(25))
    model.add(Dense(1))
    # Compiling the RNN
    model.compile(optimizer = 'adam', loss = 'mean_squared_error')
    # Fitting the RNN to the Training set
    model.fit(X_train, y_train, epochs = 10, batch_size = 1)
    print(df.iloc[0, 0] + "-model")
    model.save(df.iloc[0, 0] + "-model")
    mycursor.execute(sql_trueReady, [df.iloc[0, 0]])
    mydb.commit()

print("Con train data")
mydb = mysql.connector.connect(
  host=cf.host,
  user=cf.user,
  password=cf.password,
  database=cf.database
)
mycursor = mydb.cursor()
bootstrap_servers = ['localhost:9092']
topicName = 'train'
consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
  bootstrap_servers, auto_offset_reset='latest',
    enable_auto_commit=False)
cnt = 0
df = pd.DataFrame(columns=["company_name", "Date", "High", "Low", "Open", "Close", "Adj Close"])

for msg in consumer:
    cnt = cnt + 1
    str = msg.value.decode("utf-8")
    properties = str.split(" ")
    #print(properties)
    df.loc[len(df)] = list(properties)
    #print(pd.DataFrame(dict.fromkeys(properties)))
    if cnt == cf.train_size:
        print("Start training")
        train(df)
        cnt = 0
        df = pd.DataFrame(columns=["company_name", "Date", "High", "Low", "Open", "Close", "Adj Close"])
