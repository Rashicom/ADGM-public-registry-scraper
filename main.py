# import packages
import math
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import requests
import json
from pprint import pprint
from calendar import calendar, monthrange
from datetime import datetime, timedelta

from helper import retrieve_data,get_company_ids, get_csv_row_count, batch_fetch_campany, write_to_csv, get_last_row_date
from colorama import Fore, Back, Style

# get last updated values
captured_row_count = get_csv_row_count()
print(Fore.BLUE,f"Found {captured_row_count} existing rows in result.csv.")

# returns the date of last record which is 1st day of any month
# we have to start from the date
start_date = get_last_row_date()
print(Fore.BLUE,f"Last captured date : {start_date}.   Starting from date : {start_date}")

while True:
    """
    start an unlimited loop
    it breaks when the companie ids list returns None
    """

    # find end date
    # end date is always next month 1st day
    _,total_days_in_month = monthrange(start_date.year, start_date.month)
    remaining_days = total_days_in_month - start_date.day
    end_date = start_date + timedelta(days=remaining_days+1)
    
    # retrieve company ids
    print(Fore.MAGENTA,f"Extracting company ids from : {start_date} - to : {end_date}")
    company_ids = []
    is_empty = False
    page_number = 0
    while not is_empty:
        page_number += 1
        batch_company_ids = get_company_ids(page=page_number, page_size=10, from_date=str(start_date), to_date=str(end_date))
        print(Fore.CYAN, f"Extracted page : {page_number},  data :{batch_company_ids}")
        if len(batch_company_ids) == 0:
            is_empty = True
        else:
            company_ids.extend(batch_company_ids)
    
    # fetch company date
    print(Fore.LIGHTMAGENTA_EX)
    data = batch_fetch_campany(company_ids)

    # write to csv
    write_to_csv(data,str(end_date))

    # make start date as end date for the next loop
    start_date = end_date