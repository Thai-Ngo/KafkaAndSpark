import time
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, timedelta
import pandas as pd
from selenium.webdriver.remote import webelement

driver_location = "/usr/bin/chromedriver"
binary_location = "/usr/bin/google-chrome"

options = webdriver.ChromeOptions()
options.binary_location = binary_location
options.headless = True
options.add_argument("--window-size=1920,1080")

f = open("config.txt", 'r')
num_stock = int(f.readline())

driver = []
start_date_box = []
end_date_box = []
search_button = []
raw_data = [None] * num_stock
stock_name = []

for i in range (num_stock):
    driver.append(webdriver.Chrome(executable_path=driver_location, options=options))
    stock_name.append(str(f.read(3)))
    f.read(1)
    
    url = "https://s.cafef.vn/Lich-su-giao-dich-" + stock_name[i] + "-1.chn"
    driver[i].get(url)

    date = datetime(year=2020, month=1, day=3)
    days_added = timedelta(days=1)

    start_date_box.append(driver[i].find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate1_txtDatePicker"))
    end_date_box.append(driver[i].find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate2_txtDatePicker"))
    search_button.append(driver[i].find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_btSearch"))

    time_now = datetime.now()

while date < time_now :
    for i in range (num_stock):
        start_date_str = str(date.day) + '/' + str(date.month) + '/' + str(date.year)
        end_date_str = str(date.day) + '/' + str(date.month) + '/' + str(date.year)

        start_date_box[i].clear()
        start_date_box[i].send_keys(start_date_str)
    
        end_date_box[i].clear()
        end_date_box[i].send_keys(end_date_str)
    
        search_button[i].click()
    
        try: 
            raw_data[i] = driver[i].find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_rptData2_ctl01_itemTR").text.split(sep=" ")
            print (stock_name[i], date.date(), raw_data[i][21], raw_data[i][23], raw_data[i][19], raw_data[i][3], raw_data[i][1])
        except NoSuchElementException:
            print (stock_name[i], date.date(), raw_data[i][21], raw_data[i][23], raw_data[i][19], raw_data[i][3], raw_data[i][1])

    date += days_added
    time.sleep(2)
    
    
for i in range (num_stock):
    driver[i].close()

