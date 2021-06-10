import math
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import train_model_config as cf
import keras
from sklearn.preprocessing import MinMaxScaler

def predict(df, model):
    sc = MinMaxScaler(feature_range = (0, 1))
    for i in range(0, 3):
        X_test = df.iloc[i:, 5:6]
        sc.fit_transform(X_test)
        inputs = X_test[0:].values
        print(inputs)
        inputs = inputs.reshape(-1,1)
        inputs = sc.transform(inputs)
        X_test = []
        X_test.append(inputs[0:, 0])
        X_test = np.array(X_test)
        X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))
        print(X_test.shape)
        predicted_stock_price = model.predict(X_test)
        predicted_stock_price = sc.inverse_transform(predicted_stock_price)
        print("Date ----- Predict %s"%predicted_stock_price)
        df.loc[len(df)] = ["VIC", "2020-6-14", 0, 0, 0, predicted_stock_price[0][0], 0]


print("Main")
bootstrap_servers = ['localhost:9092']
topicName = 'predict'
consumer = KafkaConsumer (topicName, group_id ='group2',bootstrap_servers =
  bootstrap_servers, auto_offset_reset='latest',
    enable_auto_commit=False)
cnt = 0
df = pd.DataFrame(columns=["company_name", "Date", "High", "Low", "Open", "Close", "Adj Close"])
for msg in consumer:
    cnt = cnt + 1
    str = msg.value.decode("utf-8")
    properties = str.split(" ")
    df.loc[len(df)] = list(properties)
    df["Close"] = pd.to_numeric(df["Close"])
    df["Date"] = pd.to_datetime(df["Date"])
    if cnt == cf.time_steps:
        df = df.sort_values("Date", ascending=True)
        print(df)
        str = msg.value.decode("utf-8")
        properties = str.split(" ")
        model = keras.models.load_model(properties[0] + "-model")
        cnt = 0
        predict(df, model)