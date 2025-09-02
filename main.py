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

from helper import retrieve_data,get_company_ids, get_csv_row_count, batch_fetch_campany, write_to_csv
from colorama import Fore, Back, Style

# get last updated values
captured_row_count = get_csv_row_count()
print(Fore.BLUE,f"Found {captured_row_count} existing rows in result.csv.")

last_page_captured = int(captured_row_count/10)
print(Fore.BLUE,f"Last captured page no : {last_page_captured}.   Starting from page : {last_page_captured + 1}")

# target page count
target_page_count = int(input(f"how many pages( 1 page contains 10 records) you want to scrap from page number {last_page_captured} : "))

for i in range(target_page_count):
    page_number = last_page_captured+i+1
    
    # retrieve company ids
    batch_company_ids = get_company_ids(page=page_number, page_size=10)
    print(Fore.MAGENTA,f"company ids for page : {page_number} ✅")
    print(Fore.GREEN, batch_company_ids)

    # batch fetch company informations
    print(Fore.LIGHTWHITE_EX)
    data = batch_fetch_campany(batch_company_ids)


    # write data into csv
    write_to_csv(data)
    print(Fore.CYAN,f"Total records written: {10*(i+1)}")
    print(Fore.CYAN,f"Total records in Doc: {captured_row_count + (10*(i+1))}")