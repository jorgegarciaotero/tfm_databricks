# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC %pip install lxml
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/database_connector

# COMMAND ----------

# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/logger

# COMMAND ----------

import argparse
import yfinance as yf
import pandas as pd
import traceback
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import numpy as np
from datetime import datetime
import time
import logging
import lxml

# COMMAND ----------

def get_tickers_from_csv(url,skiprows,sep=','):
    '''
    Extract tickers from a remote CSV file.
    Args:
        url (str): URL of the CSV file to download.
        skiprows (int): Number of rows to skip from the top of the file.
        sep (str): Delimiter used in the CSV file (default is ',').
    Returns:
        pd.DataFrame: DataFrame containing the extracted tickers.
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Accept': 'text/csv,application/vnd.ms-excel'
    }
    session = requests.Session()
    response = session.get(url, headers=headers, allow_redirects=True)
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data, skiprows=skiprows,sep=sep,quotechar='"',thousands=",", decimal=".",on_bad_lines="skip"     )
    return df 

def process_tickers(db,table_name,logger):
    '''
    Given a list of tickers, fetch their data from Yahoo Finance and upsert it into the database.
    Args:
        db (Database): Database connection object.
        table_name (str): Name of the table to upsert data into.
        tickers (list): List of ticker symbols to process.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    '''
    #URLS
    url_russell2000 = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    url_sp500 = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
    url_eurostock = "https://www.stoxx.com/documents/stoxxnet/Documents/Reports/STOXXSelectionList/2025/April/slpublic_sxxp_20250401.csv"
    
    df_euro = get_tickers_from_csv(url_eurostock,skiprows=0,sep=';')
    df_euro = df_euro.iloc[0:600]
    df_euro = df_euro.rename(columns={'RIC':'Ticker'})
    df_euro['source']='eurostock'
    df_euro=df_euro[['Ticker','source']]
    
    df_sp500 = get_tickers_from_csv(url_sp500, skiprows=9)
    df_sp500 = df_sp500.iloc[:-2]
    df_sp500['source']='sp500'
    df_sp500=df_sp500[['Ticker','source']]
    
    df_russell = get_tickers_from_csv(url_russell2000, skiprows=9)
    df_russell = df_russell.iloc[:-2]
    df_russell['source']='rusell200'
    df_russell=df_russell[['Ticker','source']]
    
    df_all = pd.concat([df_russell,df_sp500,df_euro])
    df_all['ingest_date']=pd.to_datetime('today')
    df_all.rename(columns={'Ticker':'symbol'},inplace=True)
    df_all=df_all[['symbol','source','ingest_date']]
    df_all=df_all[df_all['symbol']!='-']
    df_all=df_all.drop_duplicates('symbol')
    upsert_data(db=db, table_name=table_name, df=df_all, pk_columns=["symbol"], logger=logger)
    print(df_all.head())
    return
    
def process_price_history(company, ticker, db, first_date, last_date,logger):
    '''
    Download and process historical stock price data for a given ticker.

    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.

    Returns:
        None
    '''
    df_hist = company.history(start=first_date, end=last_date)
    print(f"shape: {df_hist.shape}")
    print(df_hist.head())
    if not df_hist.empty:
        df_hist.reset_index(inplace=True)
        df_hist['symbol'] = ticker
        df_hist = df_hist.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        df_hist = df_hist.rename(columns={'open': 'open_v', 'close': 'close_v'})
        upsert_data(db=db, table_name="stock_data", df=df_hist, pk_columns=["date", "symbol"], logger=logger)

def process_company_info(company, db, logger):
    '''
    Retrieve and process general company information and financial ratios.

    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.

    Returns:
        None
    '''
    df_info = pd.json_normalize(company.info)
    if not df_info.empty:
        df_info = df_info.rename(columns={'52WeekChange': 'Week52Change', 'open': 'open_v'})
        df_info = df_info.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        df_info['date'] = pd.Timestamp.today().normalize()
        if 'companyofficers' in df_info.columns:
            df_info = df_info.drop(columns=['companyofficers'])
        upsert_data(db=db, table_name="company_info", df=df_info, pk_columns=["date", "symbol"], logger=logger)

def process_dividends(company, ticker, db, logger):
    """
    Extract historical dividend payments for the specified ticker.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    """
    df_div = company.dividends
    if not df_div.empty:
        df_div = df_div.to_frame().reset_index()
        df_div['symbol'] = ticker
        df_div = df_div.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        upsert_data(db=db, table_name="dividend_data", df=df_div, pk_columns=["date", "symbol"], logger=logger)

def process_splits(company, ticker, db, logger):
    """
    Extract stock split history for the specified ticker.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    """
    df_split = company.splits
    if not df_split.empty:
        df_split = df_split.to_frame().reset_index()
        df_split['symbol'] = ticker
        df_split = df_split.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        upsert_data(db=db, table_name="split_data", df=df_split, pk_columns=["date", "symbol"], logger=logger)

def process_cashflow(company, ticker, db, logger):
    """
    Retrieve and process cash flow statement data for the company.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    """
    df_cf = company.cashflow.T
    if not df_cf.empty:
        df_cf['symbol'] = ticker
        df_cf.reset_index(inplace=True)
        df_cf.rename(columns={'index': 'date'}, inplace=True)
        df_cf['date'] = pd.to_datetime(df_cf['date'])
        df_cf = df_cf.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        upsert_data(db=db, table_name="cashflow_data", df=df_cf, pk_columns=["date", "symbol"], logger=logger)

def process_recommendations(company, ticker, db, logger):
    """
    Extract analyst recommendations for the company and store them.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    """
    df_reco = company.recommendations
    if not df_reco.empty:
        df_reco['symbol'] = ticker
        df_reco = df_reco.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        df_reco['date'] = pd.Timestamp.today().normalize()
        upsert_data(db=db, table_name="recommendations_data", df=df_reco, pk_columns=["date", "symbol", "period"], logger=logger)

def process_balance_sheet(company, ticker, db, logger):
    """
    Retrieve and process balance sheet data for the company.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
    """
    df_bs = company.balance_sheet.T
    if not df_bs.empty:
        df_bs.reset_index(inplace=True)
        df_bs['symbol'] = ticker
        df_bs = df_bs.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        df_bs.rename(columns={'index': 'date'}, inplace=True)
        df_bs['date'] = pd.to_datetime(df_bs['date'])
        upsert_data(db=db, table_name="balance_sheet_data", df=df_bs, pk_columns=["date", "symbol"], logger=logger)


def process_financials(company, ticker, db, logger):
    """
    Retrieve and process the company's income statement data.
    Args:
        company (yf.Ticker): Yahoo Finance ticker object.
        ticker (str): Ticker symbol.
        db (Database): Database connection object.
        logger (Logger): Logger instance for tracking progress and errors.
    Returns:
        None
        logger (_type_): _description_
    """
    df_fin = company.financials.T
    if not df_fin.empty:
        df_fin.reset_index(inplace=True)
        df_fin['symbol'] = ticker
        df_fin = df_fin.rename(columns=lambda x: str(x).strip().lower().replace(' ', '_'))
        df_fin.rename(columns={'index': 'date'}, inplace=True)
        df_fin['date'] = pd.to_datetime(df_fin['date'])
        upsert_data(db=db, table_name="financial_data", df=df_fin, pk_columns=["date", "symbol"], logger=logger)


# COMMAND ----------

def main(first_date,last_date,batch):   
    logger = get_logger(name="my_app", level="INFO", log_file="my_app.log")
    logger.info("Starting ...")   
    
    start_time = datetime.now()
    db =  DatabaseConnector()
    df = db.read_table_from_sql("company_info")

    if check_tickers=='True':
        process_tickers(db,'tickers',logger)

    print(df.head())
    total_tickers = df.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
    print(f"Total tickers: {len(total_tickers)}")
    print(len(total_tickers))

    start, end = (batch.split(":") + [None, None])[:2]
    start = int(start) if start else None
    end = int(end) if end else None
    total_tickers = total_tickers[slice(start, end)]
    print(f"Total tickers slice: {len(total_tickers)}")

    for i, ticker in enumerate(total_tickers, start=1):
        try:
            logger.info(f"{i}/{len(total_tickers)} Processing: {ticker}")
            company = yf.Ticker(ticker)
            process_price_history(company, ticker, db,first_date, last_date, logger)
            process_dividends(company, ticker, db, logger)
            process_splits(company, ticker, db, logger)
            process_recommendations(company, ticker, db, logger)
            process_balance_sheet(company, ticker, db, logger)
            process_financials(company, ticker, db, logger)
            process_company_info(company, db, logger)
            process_cashflow(company, ticker, db, logger)
        except Exception as e:
            logger.error(f"❌ Error en {ticker}: {e}")
            continue
    end_time = datetime.now()            
    elapsed_time = end_time - start_time
    logger.info(f"⏳ Total execution time: {elapsed_time}")

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("first_date", "2025-04-17", "First Date")
dbutils.widgets.text("last_date",  "2025-04-18", "Last Date")
dbutils.widgets.text("check_tickers",  "True", "Check New Tickers")
dbutils.widgets.text("batch",  ":1000", "Batch")

first_date = dbutils.widgets.get("first_date")
last_date = dbutils.widgets.get("last_date")
check_tickers = dbutils.widgets.get("check_tickers")
batch = dbutils.widgets.get("batch")

main(first_date,last_date,batch)
