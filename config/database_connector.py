# Databricks notebook source
import yaml
from datetime import date,datetime
import traceback
import os
import pyodbc
import numpy as np
import time


class DatabaseConnector:
    """
    A class to handle SQL Server database connections and operations.
    
    Attributes:
        server (str): The server address.
        database (str): The database name.
        username (str): The username for authentication.
        password (str): The password for authentication.
        conn (pyodbc.Connection): The connection object.
    """

    def __with_retry(self, func, retries=3, delay=30, retry_message=None):
        """
        Executes a function with retries and delay.

        Args:
            func (callable): Function to be executed.
            retries (int): Number of retries.
            delay (int): Delay between retries in seconds.
            retry_message (str):Optional message to print on retry failure.

        Returns:
            Result of the function if successful, otherwise None.
        """
        for attempt in range(1, retries + 1):
            try:
                return func()
            except Exception as e:
                print(f"⚠️ Retry {attempt}/{retries}: {e}")
                if retry_message:
                    print(retry_message)
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise

    def __init__(self):
        """
        Initialize the Database class with connection settings from a YAML file.
        
        Args:
            config_file (str): Path to the YAML configuration file.
        """
        base_dir = os.getcwd()
        config_path = os.path.join(f"{base_dir}/config", "config.yml")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        self.server = config['database']['server']
        self.database = config['database']['database']
        self.username = config['database']['username']
        self.password = config['database']['password']
        self.port = config['database']['port']
        self.client_id = config['database']['client_id']
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.server}:{self.port};"
            f"database={self.database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;"
            "loginTimeout=30;"
        )
        self.connection_properties = {
            "user": self.username,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
            
    def read_table_from_sql(self, table_name, date_value=None, start_date=None, end_date=None):
        """
        Reads a table from the database, optionally filtering by a single date or a date range.

        Args:
            table_name (str): The name of the table to read.
            date_value (str): Exact date to filter (YYYY-MM-DD).
            start_date (str): Start date for range filter (inclusive).
            end_date (str): End date for range filter (inclusive).

        Returns:
            DataFrame: Spark DataFrame with the filtered result.
        """
        try:
            def _read():
                if date_value:
                    query = f"""
                        (SELECT * FROM {table_name}
                        WHERE date = '{date_value}') AS subquery
                    """
                elif start_date and end_date:
                    query = f"""
                        (SELECT * FROM {table_name}
                        WHERE date BETWEEN '{start_date}' AND '{end_date}') AS subquery
                    """
                else:
                    query = f"(SELECT * FROM {table_name}) AS subquery"
                print(f"query: \n{query}")
                df = spark.read.jdbc(
                    url=self.jdbc_url,
                    table=query,
                    properties=self.connection_properties
                )
                return df

            return self.__with_retry(
                func=_read,
                retries=5,
                delay=20,
                retry_message=f"Error reading table {table_name} from database."
            )

        except Exception as e:
            print(f"❌ Error when reading the table {table_name}: {e}")
            return None

        
    def execute_jdbc_query(self, query):
        """
        Executes a SQL statement (non-select) using Spark's JVM JDBC connection.
        Works in Databricks without needing ODBC drivers.
        """
        try:
            jvm = spark._jvm
            conn = jvm.java.sql.DriverManager.getConnection(self.jdbc_url, self.username, self.password)
            stmt = conn.createStatement()
            stmt.execute(query)
            stmt.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error executing JDBC query: {e}")



    def save_table(self, df, container, database_name, storage_account, table_name, date_value=None):
        """
        Saves a Spark DataFrame in Delta format, partitioned by date.

        If date_value is provided, only that partition will be overwritten.
        """
        try:
            client_id = self.client_id
            spark.conf.set(
                "fs.azure.account.key.smartwalletjorge.dfs.core.windows.net",
                client_id
            )

            path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{database_name}/{table_name}"

            writer = df.write.format("delta").partitionBy("date")

            if date_value:
                writer = writer.mode("overwrite").option("replaceWhere", f"date = '{date_value}'")
            else:
                writer = writer.mode("overwrite")

            writer.save(path)
            print(f"✅ Saved table {table_name} to {path}.")

        except Exception as e:
            print(f"❌ Error saving table {table_name}: {e}")
            

    def read_table_from_path(self, container, database_name, table_name, date_value=None, format="parquet"):
        """
        Reads a Delta table from a given path in Azure Data Lake Storage (ADLS).
        
        Args:
            container: The container name in ADLS.
            database_name: The name of the logical database/folder.
            table_name: The table/folder name.
            date_value: The date partition or "complete" if not partitioned.

        Returns:
            A Spark DataFrame with the table content.
        """
        try:
            client_id = self.client_id
            print(f"client_id: {client_id}")
            
            spark.conf.set(
                "fs.azure.account.key.smartwalletjorge.dfs.core.windows.net",
                client_id
            )
            
            if date_value is None:
                path = f"abfss://{container}@smartwalletjorge.dfs.core.windows.net/{database_name}/{table_name}"
            else:
                path = f"abfss://{container}@smartwalletjorge.dfs.core.windows.net/{database_name}/{table_name}/daily/{date_value}"
            
            df = spark.read.format(format).load(path)
            return df

        except Exception as e:
            print(f"Error reading the table {table_name} from path: {e}")
            return None


    
        


# COMMAND ----------

def safe_sql_value(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "NULL"
    elif isinstance(val, bool):
        return "1" if val else "0"
    elif isinstance(val, str):
        val = val.replace("'", "''")
        return f"'{val}'"
    elif isinstance(val, (pd.Timestamp, np.datetime64, datetime, date)):
        return f"'{str(val)}'"
    else:
        return str(val)

def safe_clean_cell(x):
    try:
        if isinstance(x, (dict, list, tuple)):
            return str(x)
        return x
    except Exception as e:
        print(f"❌ Error with value: {x} ({type(x)}) → {e}")
        raise

def upsert_data(db, table_name, df, pk_columns, logger):
    """
    Insert or update data into a table using JDBC-based SQL execution.

    ARGS:
        db (Database): db connection object.
        table_name (str): table's name
        df (DataFrame): pandas dataframe of data to upsert
        pk_columns (list): pk columns of the table
    RETURNS:
        None
    """
    try:
        logger.info(f"Inserting data in {table_name}")
        print(f"dataframe sample:\n{df.head(10)}")

        # Limpieza de datos
        df = df.applymap(safe_clean_cell)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        # Redondeo de floats
        for col in df.columns:
            if df[col].dtype == 'float64':
                df[col] = df[col].apply(lambda x: round(x, 6) if x is not None else None)

        # Construcción del query base con placeholders de formato
        columns = df.columns.tolist()
        on_clause = " AND ".join(f"target.{col} = source.{col}" for col in pk_columns)
        update_clause = ", ".join(f"target.{col} = source.{col}" for col in columns if col not in pk_columns)
        insert_clause = ", ".join(columns)
        values_clause = ", ".join(f"source.{col}" for col in columns)
        source_clause = ", ".join("{{{}}} AS {}".format(i, col) for i, col in enumerate(columns))  # {0} AS col1, etc.

        query_template = f"""
            MERGE INTO {table_name} AS target
            USING (SELECT {source_clause}) AS source
            ON {on_clause}
            WHEN MATCHED THEN 
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN 
                INSERT ({insert_clause})
                VALUES ({values_clause});
        """

        # Ejecutar una por una
        for row in df.itertuples(index=False, name=None):
            formatted_values = [safe_sql_value(v) for v in row]
            full_query = query_template.format(*formatted_values)
            db.execute_jdbc_query(full_query)

        logger.info(f"✅ Data upserted successfully into {table_name}!")

    except Exception as e:
        
        logger.error(f"❌ Error upserting data into: {table_name}: {e}")
        logger.debug(f"Último query que falló: {full_query if 'full_query' in locals() else 'N/A'}")
        sys.exit(0)
