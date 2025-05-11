# Databricks notebook source
# MAGIC %md
# MAGIC #### Populate the list of tickers whose information will be fetched later

# COMMAND ----------

# MAGIC %pip install lxml
# MAGIC %pip install html5lib
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/database_connector

# COMMAND ----------

# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/logger

# COMMAND ----------

import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import sys
import numpy as np
from datetime import datetime, date


# COMMAND ----------



def get_tickers_from_csv(url, skiprows=0, sep=','):
    """
    Extract tickers from a remote CSV file (like iShares ETF holdings).

    Args:
        url (str): URL of the CSV file to download.
        skiprows (int): Number of rows to skip at the top.
        sep (str): Delimiter (default ',')

    Returns:
        pd.DataFrame: Parsed DataFrame with holdings.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/csv',
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.text

        df = pd.read_csv(StringIO(content), skiprows=skiprows, sep=sep,
                         quotechar='"', thousands=",", decimal=".", on_bad_lines="skip")

        return df
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching CSV from {url}: {e}")
        return pd.DataFrame()


# COMMAND ----------

def get_ftse100_tickers_wiki():
    """
    Extract FTSE 100 tickers from Wikipedia using BeautifulSoup + pandas (Databricks-friendly).
    
    Returns:
        list: List of ticker symbols with .L suffix.
    """
    url = "https://en.wikipedia.org/wiki/FTSE_100_Index"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table", {"class": "wikitable"})

        if not tables:
            print("❌ No tables found on Wikipedia page.")
            return []

        # Buscar la tabla que tenga una columna llamada 'Ticker'
        for i, table in enumerate(tables):
            try:
                df = pd.read_html(str(table), flavor="bs4")[0]
                if "Ticker" in df.columns:
                    tickers = df["Ticker"].dropna().unique().tolist()
                    tickers = [f"{t}.L" for t in tickers]
                    return tickers
            except Exception:
                continue

        print("❌ No table with column 'Ticker' found.")
        return []

    except Exception as e:
        print(f"❌ Error fetching FTSE100 tickers: {e}")
        return []


# COMMAND ----------


def upsert_data_simple(db, table_name, df, pk_columns, logger):
    """
    UPSERT (insert or update) a DataFrame into SQL Server using a MERGE statement.

    Args:
        db: Connection object with the method execute_jdbc_query(query)
        table_name (str): Target table name
        df (pd.DataFrame): DataFrame with the data to insert or update
        pk_columns (list): List of primary key column names
        logger: Logger used to track errors and status messages
    """
    try:
        logger.info(f"🔁 Upserting {len(df)} rows into {table_name}")
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        for row in df.itertuples(index=False, name=None):
            values = []
            for val in row:
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    values.append("NULL")
                elif isinstance(val, str):
                    val = val.replace("'", "''")
                    values.append(f"'{val}'")
                elif isinstance(val, (pd.Timestamp, datetime, date, np.datetime64)):
                    values.append(f"'{str(val)}'")
                else:
                    values.append(f"'{val}'")

            columns = df.columns.tolist()
            on_clause = " AND ".join(f"target.[{col}] = source.[{col}]" for col in pk_columns)
            update_clause = ", ".join(f"target.[{col}] = source.[{col}]" for col in columns if col not in pk_columns)
            insert_columns = ", ".join(f"[{col}]" for col in columns)
            insert_values = ", ".join(f"source.[{col}]" for col in columns)
            source_values = ", ".join(f"{val} AS [{col}]" for val, col in zip(values, columns))

            query = f"""
                MERGE INTO {table_name} AS target
                USING (SELECT {source_values}) AS source
                ON {on_clause}
                WHEN MATCHED THEN 
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN 
                    INSERT ({insert_columns}) VALUES ({insert_values});
            """

            try:
                db.execute_jdbc_query(query)
            except Exception as row_err:
                logger.error(f"❌ Error upserting row {row}: {row_err}")
                logger.debug(f"Query: {query}")

        logger.info("✅ Upsert finished.")

    except Exception as e:
        logger.error(f"❌ Upsert failed: {e}")


# COMMAND ----------

# URLs de iShares
url_russell2000 = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
url_sp500 = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"

df_russell = get_tickers_from_csv(url_russell2000, skiprows=9)
df_sp500 = get_tickers_from_csv(url_sp500, skiprows=9)
ftse100_tickers=get_ftse100_tickers_wiki()
df_russell = df_russell.iloc[:-2]
df_sp500 = df_sp500.iloc[:-2]

df_sp500['source']='sp500'
df_russell['source']='rusell200'
df_ftse100=pd.DataFrame(ftse100_tickers,columns=['Ticker'])
df_ftse100['source']='ftse100'
df_all=pd.concat([df_sp500,df_russell,df_ftse100],ignore_index=True)
df_all['ingest_date']=pd.to_datetime('today')

df_all.rename(columns={'Ticker':'symbol'},inplace=True)
df_all=df_all[['symbol','source','ingest_date']]

df_all=df_all[df_all['symbol']!='-']
df_all=df_all.drop_duplicates('symbol')
print(f"shape: {df_all.shape}")

db =  DatabaseConnector()
for col in df_all.columns:
    df_all[col] = df_all[col].apply(lambda x: str(x) if isinstance(x, (dict, list, tuple)) else x)
logger = get_logger(name="my_app", level="INFO", log_file="retrieve_tickets.log")

upsert_data_simple(
    db=db,
    table_name="tickers",
    df=df_all,
    pk_columns=["symbol"],  # ← tu clave primaria
    logger=logger
)

# COMMAND ----------


