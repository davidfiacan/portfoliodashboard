# Final review 12-02 - FINAL

# Non-standard libraries - batch install using 'requirements.txt' 
import pandas as pd  # pip install pandas
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3

# Standard libraries
import datetime as dt
import numpy as np
import base64
from io import StringIO, BytesIO 

# Import project's modules
from tl_sql_queries import *

DASHDATE = dt.date(2022,12,31)

def gencsvdownload(df, name):
    """
    Returns st.markdown CSV download link for dataframe passed
    
    params:
    -------
    df: pandas dataframe: df to export as CSV
    """
    
    # Credit Excel: https://discuss.streamlit.io/t/how-to-add-a-download-excel-csv-function-to-a-button/4474/5
    towrite = BytesIO()
    df.to_csv(towrite, encoding="utf-8", index=False, header=True)  # write to BytesIO buffer
    towrite.seek(0)  # reset pointer
    b64 = base64.b64encode(towrite.read()).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{name}.csv">Export {name} as CSV</a>'
    return st.markdown(href, unsafe_allow_html=True)

def eomonth(date, months):
    """
    Excel 'EOMONTH' function - returns last date of Nth month offset by
        number of months N
        
    params
    ------
    date: datetime.datetime.date
    months: int: number of months to offset
    """
    
    months = int(months)
    y, m = divmod(date.month + months + 1, 12)
    if m==0:
        y -= 1
        m = 12
    return (dt.datetime(date.year + y, m, 1) - dt.timedelta(days=1)).date()

def printmessages(msglist):
    """
    Prints messages from a list passed 
        and restates the global messages list
        back to an empty list
        
    params:
    ------
    msglist: list: list of messages to iterate through and print
    """
    
    [print(msg) for msg in msglist]
    global messages; messages = []

def calcequity_systems(db, per, systems, sysallocs, capital): 
    """
    Calculates equity values (both absolute and percentage) for trading systems as specified by user 
        (can be single system, multiple systems, or all) for the period specified by the user (using multi-select in the
        left sidebar). 
        Percentage equities are plotted on this page in section 1, whereas absolute equities are used for system correlation
        matrix in section 2 of this page
        
    params:
    ------
    per: str: equity filter period string
    systems: str: systems string in SQL format that's needed to return equities for specific systems (columns) from database
    sysallocs: dict: dict of percentage allocations per system (using SYSALLOC const from this page)
    capital: int: absolute value of capital to base the equity percent returns on (using CAPITAL const from this page)
    """
        
    # Execute SQL query - closed and open P/L by business date by system
    df = db.do_execsqlpd_r(SQLQ_SYSTEMEQUITY(systems,per,DASHDATE), datecols=['DATE'])
    
    # Clean up data after SQL query; drop excessive cols and rows, replace placeholder values with 0'
    df.columns = df.columns.str.replace('"','')
    df = (df.replace({"PL_CLOSED":float(0),"PL_OPEN":float(0)})
            .drop(['PL_TOTAL'],axis=1)
            .set_index('SYSTEM')
            .drop('SYSTEM')
            .reset_index(drop=False))
    
    # Create pivot table by system 
    pivot = pd.pivot_table(df, index='DATE', columns=df['SYSTEM'], values=['PL_CLOSED','PL_OPEN'], aggfunc=sum, fill_value=0)
    
    # Absolute values equity - cumulative sum of closed P/L by system + its open P/L at end of each date
    # -- then add initial capital for each system using const SYSALLOC and CAPITAL
    # -- using numpy vectorize to calculate this - faster than doing this with pandas .add method (pd 13ms, np 3.6ms)
    equity_abs = pivot['PL_CLOSED'].cumsum() + pivot['PL_OPEN']
    def addcapital(sys, values):
        return values + (sysallocs[sys] * capital)
    for s in equity_abs.columns: 
        equity_abs[s] = np.vectorize(addcapital)(s, equity_abs[s])

    # Construct final equity - percentage return values
    # -- use absolute values to calculate daily % change 
    # -- then calculate cumualtive % return using cumprod method
    equity_pct = equity_abs.pct_change().fillna(0)
    equity_pct = ((1 + equity_pct).cumprod() - 1) * 100
    
    return equity_pct.reset_index(), equity_abs.reset_index()
