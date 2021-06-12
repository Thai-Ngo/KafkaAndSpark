from time import time
import mysql.connector as sql
from datetime import date, datetime, timedelta
from mysql.connector.fabric.connection import FABRICS
from numpy.core.fromnumeric import size
from os import system, name
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


# Prpare data
db_connection = sql.connect(
  host= "localhost",
  user= "root",
  password= "password",
  database= "stock"
)
db_cursor = db_connection.cursor()
db_cursor.execute('SELECT * FROM prediction')
table_rows = db_cursor.fetchall()
predict_df = pd.DataFrame(table_rows)

db_cursor = db_connection.cursor()
db_cursor.execute('SELECT * FROM stockrecord')
table_rows = db_cursor.fetchall()
stock_df = pd.DataFrame(table_rows)


f = open("config.txt", 'r')
num_stock = int(f.readline())
stock_list = []

today = date(year=2020, day=20, month=3)
limit_day = timedelta(days=30)

for i in range (num_stock):
    stock_list.append(f.read(3))
    f.read(1)

# Processing
if name == 'nt':
    system('cls')
    
        # for mac and linux(here, os.name is 'posix')
else:
    system('clear')
print ("STOCK DEMO APPLICATION WITH KAFKA AND SPARK")

while True:
    input_str = input(">>> $ ")
    if input_str == "exit" :
        break
    elif input_str == "list" :
        for i in range (num_stock):
            print (str(i + 1) + ".", stock_list[i])
    elif input_str == "clear" :
                # for windows
        if name == 'nt':
            system('cls')
    
        # for mac and linux(here, os.name is 'posix')
        else:
            system('clear')
        print ("STOCK DEMO APPLICATION WITH KAFKA AND SPARK")
    else :
        list_str = input_str.split(sep=' ')
        if len(list_str) == 2 and list_str[0] == "stockinfo" :
            if list_str[1] in stock_list :
                new_df = stock_df[(stock_df[1] <= today) & (stock_df[1] >= today - limit_day) & (stock_df[0] == list_str[1])]
                new_df = new_df.sort_values(by=[1], ignore_index=True, ascending=False)
                new_df = new_df.rename(columns={0: 'Name', 1 : 'Date', 2 : 'High', 3 : 'Low', 4 : 'Open', 5 : 'Close', 6 : 'Adjucted close'})
                print (new_df)
            else :
                print ("Not found this stock")
        elif len(list_str) == 3 and list_str[0] == "predict" and list_str[1] == "stockinfo":
            if list_str[2] in stock_list :
                new_df = predict_df[(predict_df[2] == today) & (predict_df[0] == list_str[2])]
                new_df = new_df.sort_values(by=[1], ignore_index=True, ascending=True)
                new_df = new_df.rename(columns={0: 'Name', 1 : 'Date', 3 : 'Close'})
                new_df = new_df.drop(columns=[2])
                print (new_df)
            else :
                print ("Not found this stock")
        elif len(list_str) == 2 and list_str[0] == "graph" :
            if list_str[1] in stock_list:
                new_df = stock_df[(stock_df[1] <= today) & (stock_df[1] >= today - limit_day) & (stock_df[0] == list_str[1])]
                new_df = new_df.sort_values(by=[1], ignore_index=True, ascending=True)
                new_df = new_df.rename(columns={ 1 : 'Date', 2 : 'High', 3 : 'Low', 4 : 'Open', 5 : 'Close', 6 : 'Adjucted close'})
                new_df = new_df.drop(columns=[0])
                
                trace1 = {
                    'x': new_df.Date,
                    'open': new_df.Open ,
                    'close': new_df.Close,
                    'high': new_df.High,
                    'low': new_df.Low,
                    'type': 'candlestick',
                    'name': list_str[1],
                    'showlegend': True                    
                }
                
                # Config graph layout
                my_layout = go.Layout({
                    'title': {
                        'text': list_str[1] + ' Moving Averages',
                        'font': {
                            'size': 15
                        }
                    }
                })
                
                fig = go.Figure(data=[trace1], layout=my_layout)
                fig.show()
                
            else :
                print ("Not found this stock")
        
        elif len(list_str) == 3 and list_str[0] == "predict" and list_str[1] == "graph" :
            if list_str[2] in stock_list :
                
                new_predict = predict_df[(predict_df[2] == today) & (predict_df[0] == list_str[2])]
                new_predict = new_predict.sort_values(by=[1], ignore_index=True, ascending=True)
                new_predict = new_predict.rename(columns={0: 'Name', 1 : 'Date', 3 : 'Close'})
                new_predict = new_predict.drop(columns=[2])
                
                new_stock = stock_df[(stock_df[1] <= today) & (stock_df[1] >= today - limit_day) & (stock_df[0] == list_str[2])]
                new_stock = new_stock.sort_values(by=[1], ignore_index=True, ascending=True)
                new_stock = new_stock.rename(columns={0: 'Name', 1 : 'Date', 5 : 'Close'})
                new_stock = new_stock.drop(columns=[2,3,4,6])
                
                append_df = new_stock[new_stock.Date == today]
                new_predict = new_predict.append(append_df)
                new_predict = new_predict.sort_values(by=['Date'], ignore_index=True, ascending=True)

                my_layout = go.Layout({
                    'title': {
                        'text': 'Close price chart of ' + list_str[2],
                        'font': {
                            'size': 15
                        }
                    }
                })
                fig = go.Figure(layout=my_layout)

                fig.add_trace(go.Scatter(
                    x= new_stock.Date,
                    y= new_stock.Close,
                    name = 'Reality close price',
                    mode='lines'
                ))
                fig.add_trace(go.Scatter(
                    x= new_predict.Date,
                    y= new_predict.Close,
                    name = 'Predict close price',
                    mode='lines+markers'
                ))

                fig.show()
            else :
                print ("Not found this stock")        
        else:
            print ("Command not found")
print("BYE")


  
