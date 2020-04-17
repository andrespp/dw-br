"""datasrc_mssql.py
"""
import pyodbc
import pandas as pd
import pandas.io.sql as sqlio

class DatasrcMssql():
    """DatasrcMssql class
    """

    def __init__(self, name, dbms, host, port, base, inst, user, pswd):
        # Define class attributes
        self.name = name
        self.dbms = dbms
        self.host = host
        self.port = port
        self.base = base
        self.inst = inst
        self.user = user
        self.pswd = pswd

    def mssql_conn(self, host, port, dbname, instance, user, pwd):
        """Connects to MSSQL database

        Parameters
        ----------
            host : str
                server name or ip address
            port : int
                server port
            dbname : srt
                database name
            instance : str
                SQL Server instance ('SQLEXPRESS')
            user : srt
                database user
            pwd : srt
                database user's password

        Returns
        -------
            conn :
            Database connection or -1 on error
        """
        driver = '{ODBC Driver 17 for SQL Server}'
        server = host + ',' + port
        conn_str = "DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
            driver, server, dbname, user, pwd)
        try:
            conn = pyodbc.connect(conn_str)

            return conn
        except:
            return -1

    def get_conn(self):
        """Connects to the Datasrc Database

        Returns
        -------
            conn :
            Database connection or -1 on error
        """
        return self.mssql_conn(self.host, self.port, self.base, self.inst,
                               self.user, self.pswd)

    def test_conn(self, verbose=False):
        """Checks if Datasrc's DBMS is reachable.

        Returns
        -------
            status : boolean
                True if database is reachable, False otherwise
        """
        conn = self.get_conn()

        if conn == -1: # DW DB Unreachable
            return False

        else:
            if verbose:
                cur = conn.cursor()
                cur.execute("SELECT @@version;")
                row = cur.fetchone()
                while row:
                    print(row[0])
                    row = cur.fetchone()
            conn.close()
            return True


    def query(self, query="SELECT"):
        """Connects to the Datasrc DB and run defined query

        Parameters
        ----------
            query : str
                Desired query

        Returns
        -------
            df : DataFrame
                Resulting Dataframe (Empty dataframe if unable to connect)
        """

        ## Connect to an existing database
        conn = self.get_conn()

        if conn == -1:
            print("query(): Unable to connect to the database.")
            return pd.DataFrame()

        ## Perform query
        df = sqlio.read_sql_query(query, conn)

        ## Close communication with the database
        conn.close()

        return df
        #return

if __name__ == '__main__':
    print('Load data Module')
