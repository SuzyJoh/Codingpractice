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
            #print(data_dict)
            df1 = pd.DataFrame(data_dict, index = [0])
            df = df._append(data_dict, ignore_index = True)
    return df

def transform(df):
    exchange_rate = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    #print (df['MC_EUR_Billion'][4])
    return df

def load_to_csv(df, output_path):
    df.to_csv(csv_path)
	
def load_to_db(df, sql_connection, table_name):
    sql_connection = sqlite3.connect('Banks.db')
    df.to_sql('Largest_banks', sql_connection, if_exists = "replace", index = False)

def run_query(query_statement, sql_connection):
    query_statement = f"SELECT * FROM Largest_banks"
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

    query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
   
    query_statement = f"SELECT Name from Largest_banks LIMIT 5"
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

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
run_query(query_statement, sql_connection)
   
log_progress ('Process Complete.')
sql_connection.close()
log_progress ('Server Connection closed')