# Final review 12-02 - FINAL

# Non-standard libraries - batch install using 'requirements.txt' 
import pandas as pd  # pip install pandas
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3

# Standard libraries
import datetime as dt
import numpy as np
import os

# Import project's modules
from tl_sql_tabledefs import *

# Constants needed for this module
DBPATH = os.path.join(os.getcwd(),'database','db.db')
DATAPATH = os.path.join(os.getcwd(),'data')

class Db():
    """
    Database class used to connect to and interact with database 
    """
    
    def __init__(self):
        """
        Init method - called when class is instantiated
        """
        
        self.do_connect()
        self.do_createtables()

    def do_connect(self):
        """
        Connects to SQLite database using DBPATH constant
        """
        
        try:
            self.conn = sqlite3.connect(DBPATH,timeout=5)
        except:
            self.conn = None
            
    def do_createtables(self):
        """
        Creates SQL tables using definitions from 'tl_sql_tabledefs' script
        Tables are created only if non-existent, and so if a table already exists, it
            is not overwritten
        """ 
        
        c = self.conn.cursor()
        c.execute(TABLE_TRADESALL)
        c.execute(TABLE_BENCHMARK)
        c.execute(TABLE_OPENEQUITY)
        
    def do_execsqlpd_w(self,df,table,if_exists,**kwargs):
        """
        Writes to database using pandas 'to_sql' method. Supports any number of keyword args
        
        params:
        ------
        self: Db class instance
        df: pd.DataFrame: df to insert into database
        table: str: table name to insert data to
        if_exists: str: ‘fail’, ‘replace’, ‘append’
        kwargs: any number of optional keyword arguments to unpack
        """
        
        df.to_sql(table, self.conn, if_exists=if_exists, index=False, **kwargs)
    
    def do_execsqlpd_r(self, sql, *sqlparams, datecols=None):
        """
        Reads from database using pandas 'read_sql' method
        
        params:
        -------
        self: Db class instance 
        sql: str: SQL query definition
        sqlparams: tuple: any number of SQL params to unpack
        datecols: list: list of df column labels to be converted to dt.date object, default None
        """
        
        df = pd.read_sql(sql, self.conn, params=sqlparams)
        
        # Convert datecols to dt.date if datecols passed
        if datecols:
            for cname in datecols:
                exec(f"df['{cname}'] = pd.to_datetime(df['{cname}'], dayfirst = True).dt.date")
        return df
        
    def do_execsql(self, sql, exectype, *sqlparams):
        """
        Writes to database using SQL query passed, supports any number of SQL parameters
        
        Params
        ------
        self: Db class instance 
        sql: str: SQL statement
        exectype: str: {'read','write'}
            'read' - reads and returns the data from database in form of pd DataFrame
            'write' - inserts or updates table in database
        sqlparams: tuple: any number of SQL params
        
        on error returns 'error' string
        """
        
        if exectype == 'write':
            self.conn.cursor().execute(sql, sqlparams)
            self.conn.commit()
        else:
            return 'error'
        
#     def do_execsqlmany(self,sql,sqlparams):
#         """
#         Writes multiple records to SQL database using executemany statement
        
#         Params
#         ------
#         self: Db class instance 
#         sql: str: SQL statement
#         sqlparams: tuple: any number of SQL params
#         """
        
#         self.conn.cursor().executemany(sql, sqlparams)
#         self.conn.commit()
        