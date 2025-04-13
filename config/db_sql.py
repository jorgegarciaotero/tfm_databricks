import pyodbc
import yaml
import os
import sys
import traceback
class Database:
    """
    A class to handle SQL Server database connections and operations.
    
    Attributes:
        server (str): The server address.
        database (str): The database name.
        username (str): The username for authentication.
        password (str): The password for authentication.
        conn (pyodbc.Connection): The connection object.
    """

    def __init__(self, config_file="config.yml"):
        """
        Initialize the Database class with connection settings from a YAML file.
        
        Args:
            config_file (str): Path to the YAML configuration file.
        """
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        self.server = config['database']['server']
        self.database = config['database']['database']
        self.username = config['database']['username']
        self.password = config['database']['password']
        self.conn = None

    def connect(self):
        """
        Establishes a connection to the database.
        If the connection is already established, it does nothing.
        """
        try:
            if not self.conn:
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
                self.conn = pyodbc.connect(conn_str)
                print("Connection established successfully")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close(self):
        """
        Closes the database connection if it's open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            print("Connection closed")

    def execute_query(self, query, params=None):
        """
        Executes a SELECT query and returns the results.

        Args:
            query (str): The SQL query string.
            params (tuple): The parameters to pass to the query (if any).

        Returns:
            list: A list of rows returned by the query.
        """
        try:
            self.connect()
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def execute_non_query(self, query, params=None):
        """
        Executes an INSERT, UPDATE, or DELETE query.
        Commits the transaction after execution.

        Args:
            query (str): The SQL query string.
            params (tuple): The parameters to pass to the query (if any).
        """
        try:
            self.connect()
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error executing non-query operation: {e}")
            traceback.print_exc() 
            print("params: ",params)
            print('---')
            print(query)
            sys.exit(0)

if __name__ == "__main__":
    db = Database()
    db.connect()
    # Test SELECT query
    result = db.execute_query("SELECT name FROM sys.tables")
    for row in result:
        print(row)

    db.close()
