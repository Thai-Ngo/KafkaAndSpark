import time
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer
import sys
# driver_location = "/usr/bin/chromedriver"
# binary_location = "/usr/bin/google-chrome"

options = webdriver.ChromeOptions()
options.headless = False
options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(executable_path="chromedriver", options=options)
driver.get("https://s.cafef.vn/Lich-su-giao-dich-VIC-1.chn")

date = datetime(year=2020, month=1, day=3)
days_added = timedelta(days=1)

start_date_box = driver.find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate1_txtDatePicker")
end_date_box = driver.find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate2_txtDatePicker")
search_button = driver.find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_btSearch")

time_now = datetime.now()
raw_data = ""
while date < time_now :
    start_date_str = str(date.day) + '/' + str(date.month) + '/' + str(date.year)
    end_date_str = str(date.day) + '/' + str(date.month) + '/' + str(date.year)

    start_date_box.clear()
    start_date_box.send_keys(start_date_str)
    
    end_date_box.clear()
    end_date_box.send_keys(end_date_str)
    search_button.click()
    
    time.sleep(2)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    try: 
        raw_data = driver.find_element_by_id("ctl00_ContentPlaceHolder1_ctl03_rptData2_ctl01_itemTR").text.split(sep=" ")
        trim_data = f'{"VIC"} {date.date()} {raw_data[21]} {raw_data[23]} {raw_data[19]} {raw_data[3]} {raw_data[1]}'
        print (trim_data)
    except:
        print("Missing")
        trim_data = f'{"VIC"} {date.date()} {raw_data[21]} {raw_data[23]} {raw_data[19]} {raw_data[3]} {raw_data[1]}'
        print (trim_data)
    # finally:
        # try:
        #     producer.send('stock-rec', value=trim_data.encode("utf-8"))
        # except:
        #     e = sys.exc_info()[0]
        #     print("Error: %s"%e)

    date += days_added