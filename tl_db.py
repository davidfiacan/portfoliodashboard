import pandas as pd  # pip install pandas
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3
import yfinance as yf # pip install yfinance
import datetime as dt
import numpy as np
import os
# import streamlit as st  # pip install streamlit

# Import modules
from tl_sql_tabledefs import *

DBPATH = os.path.join(os.getcwd(),'database','db.db')
DATAPATH = os.path.join(os.getcwd(),'data')

class Db():
    """
    Database class used to connect to and interact with database 
    """
    
    def __init__(self):
        self.do_connect()
        self.do_createtables()

    def do_connect(self):
        """
        Connects to database
        Sets error flag to True if not successful
        """
        try:
            self.conn = sqlite3.connect(DBPATH,timeout=5)
        except:
            self.conn = None
            
    def do_createtables(self):
        """
        Creates SQL tables; table definitions are imported from 'tl_sql_tabledefs' file
        """ 
        
        c = self.conn.cursor()
        c.execute(TABLE_TRADESALL)
        c.execute(TABLE_BENCHMARK)
        c.execute(TABLE_OPENEQUITY)
        
#     def do_updatebenchmark(self, symbol, period):
#         """
#         Downloads stock data from Yahoo Finance, saves as CSV in DATAPATH folder,
#             inserts this data into 'benchmark_spy' table in the database (overwrites existing data)
#         More info see https://pypi.org/project/yfinance/

#         params:
#         -------
#         symbol: str: ticker to donwload
#         period: str: period to download; valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max    
#         """
#         # Download data from yahoo
#         data = yf.Ticker(symbol).history(period=period)
#         data = data.reset_index(drop=False).dropna()
#         data.columns = data.columns.str.upper()
#         if 'ADJ CLOSE' in data.columns:
#             data = data.drop('CLOSE', axis=1)
#             data = data.rename({'ADJ CLOSE':'CLOSE'})

#         # Drop excessive cols
#         data = data.drop(['DIVIDENDS','VOLUME','STOCK SPLITS'],axis=1)

#         # Format dates as dt.date objects and add MM and YYYY cols
#         data['DATE'] = data['DATE'].apply(lambda d: d.date()) 
#         data.insert(len(data.columns),"MONTH",data['DATE'].apply(lambda d: d.month))
#         data.insert(len(data.columns),"YEAR",data['DATE'].apply(lambda d: d.year))

#         # Save as CSV
#         data.to_csv(os.path.join(DATAPATH,f'{symbol}.csv'),index=False)
        
#         # Then insert into database
#         self.do_execsqlpd_w(data,'benchmark_spy','replace')
    
    def do_execsqlpd_w(self,df,table,if_exists,**kwargs):
        """
        Writes to database using pandas 'to_sql' method
        
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
        Retrieves data from database using pandas 'read_sql' method
        
        params:
        -------
        self: Db class instance 
        sql: str: SQL query definition
        sqlparams: tuple: any number of SQL params
        datecols: list: list of df column labels to converted to dt.date object, default None
        """
        
        df = pd.read_sql(sql,self.conn,params=sqlparams)
        
        # Convert datecols to dt.date if datecols passed
        if datecols:
            for cname in datecols:
                exec(f"df['{cname}'] = pd.to_datetime(df['{cname}'], dayfirst = True).dt.date")
        return df
        
    def do_execsql(self,sql,exectype,*sqlparams):
        """
        Writes to/reads from database using SQL query passed
        
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
        
        if exectype == 'read':
            data = pd.read_sql(sql,self.conn,params=sqlparams)
            return data
        elif exectype == 'write':
            self.conn.cursor().execute(sql, sqlparams)
            self.conn.commit()
        else:
            return 'error'
        
    def do_execsqlmany(self,sql,sqlparams):
        """
        Writes multiple records to SQL database using executemany statement
        
        Params
        ------
        self: Db class instance 
        sql: str: SQL statement
        sqlparams: tuple: any number of SQL params
        """
        
        self.conn.cursor().executemany(sql, sqlparams)
        self.conn.commit()
        