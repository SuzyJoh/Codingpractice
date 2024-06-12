import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
from datetime import datetime


url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './Largest_banks_data.csv'
db_name='Banks.db'
table_name = 'Largest_banks'


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime (timestamp_format)
    with open ("./code_log.txt", "a") as f:
        f.write(timestamp + ':' + message + '\n')
   
def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
         if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict={"Name": bank_name, "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index =[0])
            df = pd.concat([df, df1], ignore_index = True)
    return df
df = extract(url, table_attribs)

def transform(df):
    exchange_rate = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]

    return df
test = transform(df)
test

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
	


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''








def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress ('Preliminaries complete. Initiating ETL process.')
df = extract (url, table_attribs)
   
log_progress ('Data extraction complete.  Initiating Transformation process.')
df = transform (df)
   
log_progress ('Data transformation complete. Initiating Loading process.')
load_to_csv (df, csv_path)
   
log_progress ('Data saved to csv file.')
sql_connection = sqlite3.connect('Banks.db')
   
log_progress ('SQL Connection initiated.')
load_to_db (df, sql_connection, table_name)
   
log_progress ('Data loaded to Database as a table. Executing queries.')
run_query (query_statement, sql_connection)
   
log_progress ('Process Complete.')
sql_connection.close()
log_progress ('Server Connection closed')