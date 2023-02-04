import pandas as pd  # pip install pandas
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3
import yfinance as yf # pip install yfinance
import datetime as dt
import numpy as np
import base64
from io import StringIO, BytesIO 


from tl_sql_queries import *


# DASHDATE = dt.datetime.now().date()
DASHDATE = dt.date(2022,12,31)
FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -12, #-DASHDATE.month
               'All' : None}


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
    Excel 'EOMONTH' function - returns last date of the month offset by
        number of months passed
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
    """
    
    [print(msg) for msg in msglist]
    global messages; messages = []

def calcequity_systems(per, systems, sysallocs, capital): 
        
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

    
# OLD BACKUP - MAIN PAGE
#-------------------------------------------------------------------------------------

def filt_monthlytable(monthly, per):
    """
    Filters passed monthly profit table df and returns its filtered copy
    
    params:
    ------
    monthly: pd DataFrame: monthly profit table data to be filtered
    per: str: period filter specified by user, using FILTERS_PER constant
    """
        
    return monthly.iloc[FILTERS_PER[per]:].reset_index(drop = True)

def calcequity_portfolio(openpl, cap, per, plotb, bdata=None):
    """
    Calculates cumulative return in % for df passed, using its
        'PL' column
    
    params:
    -------
    df: pd.dataframe
    cap: int: initial capital to base the return % calc on
    per: str: str in FILTERS constant used as a lookback period
    plotb: bool: user's option to plot benchmark's equity (S&P500 - SPY) 
    """
    
    # Filter passed df on lookback period selected by user
    closedpl = filt_trades(db, per) # << rename to retrievetrades later
    
    # Re-sort closed P/L data (data retrieved by SQL query ordered in descdeding manner)
    # closedpl = closedpl.sort_values(by=['ENTRY DATE','EXIT DATE']).reset_index(drop=True)

    # Set range of all business dates in closedpl dataframe 
    daterange = pd.bdate_range(start = eomonth(min(closedpl['ENTRY DATE']),-1), 
                               end = eomonth(max(closedpl['ENTRY DATE']),0))

    # Create new df with all business dates
    alldates = pd.DataFrame(data=daterange,columns=['DATE'])
    alldates['DATE'] = alldates['DATE'].apply(lambda d: d.date())

    # SQL-style join alldates with closedpl 
    # outer join to get sum of total closed PL across ALL business dates; if 0 then no realised P/L on current date
    closedpl = alldates.merge(closedpl,
                              left_on='DATE',
                              right_on='EXIT DATE',
                              how='outer').groupby(by=['DATE'],dropna=False)['PL'].agg('sum').reset_index() 
    closedpl = closedpl.dropna()
    
    # SQL-style join open P/L and closed P/L; outer join to get ALL dates that appear in both dataframes; fill N/As with 0's 
    equity = closedpl.merge(openpl,on='DATE',how='left',suffixes=['_CLOSED','_OPEN']).fillna(0)
    
    # Calc cumulative equity as initial capital + cumulative realised P/L + any open P/L on given day
    equity['EQ_TOTALCUM'] = cap + (equity['PL_CLOSED'].cumsum() + equity['PL_OPEN'])
    
    # Calc daily % change in equity and cumulative % return
    equity['RET%_DAILY'] = equity['EQ_TOTALCUM'].pct_change().fillna(0) * 100
    equity['RET%_CUM'] = ((1 + equity['RET%_DAILY']/100).cumprod() - 1) * 100
    
    # Join benchmark data if option selected
    if plotb:
        
        # Join with benchmark data
        equity = equity.merge(bdata, how='left', on='DATE').fillna(method='ffill')
        equity = equity.rename({'CLOSE':'SPY_CLOSE'},axis=1)
        
        # Calc metrics for benchmark
        equity['RET%_SPY_DAILY'] = equity['SPY_CLOSE'].pct_change().fillna(0) * 100
        equity['RET%_SPY_CUM'] = ((1 + equity['RET%_SPY_DAILY']/100).cumprod() - 1) * 100

    return equity

def calmonthlypl(mthly_closedpl, daily_openpl, cap, per, plotb, bdata=None):
    """
    Calculates monthly return metrics for a monthly profit table dataframe passed
        with an optional benchmark comparison
    
    params:
    -------
    mthly_closedpl: pd.DataFrame: portfolio returns data in monthly resolution (this is handled via SQL query)
    daily_openpl: pd.DataFrame: portfolio open P/L by day data
    cap: int or float: starting capital of a portfolio to base the cumulative return % on
    per: str: period filter specified by user, using FILTERS_PER constant
    plotb: bool: user's option to include benchmark's monthly returns for comparison (S&P500 - SPY) 
    bdata: pd.DataFrame: benchmark returns data in monthly resolution (this is handled via SQL query)
    """
    
    # Transform daily open P/L data to monthly and keep last monthly open P/L value
    daily_openpl['YYYY_MM'] = daily_openpl['DATE'].apply(lambda d: d.strftime("%Y")) + '-' + daily_openpl['DATE'].apply(lambda d: d.strftime("%m"))
    daily_openpl = daily_openpl.drop_duplicates(subset=['YYYY_MM'],keep='last').reset_index(drop=True)

    # Then join mthly_closedpl and open P/L that's now transformed into monthly granulation
    monthly = mthly_closedpl.merge(daily_openpl,how='outer',left_on='YYYY_MM',right_on='YYYY_MM')
    
    # Then restate P/L at end of the month to include open P/L as at last trading day of the month; drop excessive cols
    monthly['P/L - portfolio'] = monthly['P/L - portfolio'] + monthly['PL']
    monthly = monthly.drop(['DATE','PL'], axis=1)
    
    # Filter merged dataframe using period specified by user
    monthly = filt_monthlytable(monthly, per)
    
    # If SPY comparison, then do SQL-style join on both dataframes
    if plotb:
        monthly = monthly.merge(bdata, how='left', on='YYYY_MM')
        
    # Filter merged dataframe using period specified by user
    # monthly = filt_monthlytable(monthly, per)
    
    # Get SPY's starting close price (close of month that preceds first filtered month)
    if plotb:
        firstmonth = monthly.loc[0,'YYYY_MM']
        prevmonth = monthly_spy.query("YYYY_MM==@firstmonth")
        spy_start = float(bdata.iloc[prevmonth.index - 1]['Equity - SPY'])
        
    # Portfolio equity (SPY already done)
    monthly['Equity - portfolio'] = cap + monthly['P/L - portfolio'].cumsum()
    
    # Calculate monthly return %
    calccols = ['portfolio', 'SPY'] if plotb else ['portfolio']
    for c in calccols:
        
        # Calc monthly return
        exec(f"monthly['Return % - {c}'] = monthly['Equity - {c}'].pct_change().fillna(0)")
        
        # Handle first row's % change - capital for equity, SPY preceding month's closing price 
        x = cap if c == 'portfolio' else spy_start
        exec(f"monthly.loc[0,'Return % - {c}'] = (monthly.loc[0,'Equity - {c}'] / x) - 1")
        
        # Then calculate cumulative return %
        exec(f"monthly['Cum % - {c}'] = (1 + monthly['Return % - {c}']).cumprod() - 1")
        
    # drop excessive columns
    # excescols = ['P/L - portfolio', 'Equity - portfolio'] 
    # if plotb: 
    #     excescols.append('Equity - SPY')
    # monthly = monthly.drop(excescols, axis=1)
    
    # Format as percentages
    for c in calccols:
        exec(f"monthly['Return % - {c}'] = monthly['Return % - {c}'] * 100")
        exec(f"monthly['Cum % - {c}'] = monthly['Cum % - {c}'] * 100")
    
    return monthly


# OLD BACKUP - SYSTEM DETAIL PAGE
#-------------------------------------------------------------------------------------

def filt_trades(db, f_per, f_sys = None):
    """
    Retrieves closed trades data from 'trades' table using db class do_execsqlpd_r method
        using user's filters passed. If f_sys system filter not passed, then SQL statement
        retrieves data for all systems
    
    params:
    ------
    db: Db class instance
    f_per: str: period filter specified by user, using FILTERS_PER constant
    f_sys: default None: optionally can filter based on trading system (supports multiselect)
    """
    
    # System/s filter - if 'All' or not provided then set to SELECT statement to get all systems
    # Else - convert f_sys list to SQLite format - string with regular brackets 
    if not f_sys:
        f_sys = '(SELECT DISTINCT [SYSTEM] FROM trades)'
    
    # Period filter - if 'All' then set to 2000 to ensure all dates are covered, else get start date using eomonth function
    # -- then convert to string in SQLite format (YYYY-MM-DD)
    # f_per = dt.date(2000,1,1) if f_per == 'All' else eomonth(DASHDATE,FILTERS_PER[f_per])
    # f_per = f_per.strftime("%Y-%m-%d")

    # Then execute SQL statement using user filters
    return db.do_execsqlpd_r(QUERIES_SYSDETAIL['filt_trades'](f_sys, f_per),datecols=['ENTRY DATE', 'EXIT DATE'])


def calcequity_systems(closedpl, openpl, cap, sysallocs, per, systems): 
    
    # Filter closed P/L data on period on systems passed
    # - then sort chronologically by dates 
    # - then rename exit date col to date so that the name is consistent with other DFs 
    closedpl = (filt_trades(closedpl, per, systems)
                .sort_values(by=['ENTRY DATE','EXIT DATE'])
                .reset_index(drop=True)
                .rename({'EXIT DATE':'DATE'},axis=1))
    
    # Filter open P/L data as well 
    openpl = filt_trades(openpl.rename({'DATE':'ENTRY DATE'},axis=1), per, systems).rename({'ENTRY DATE':'DATE'},axis=1)
    
    # Get all business dates that fall between earliest and latest date of closed P/L dataframe
    daterange = pd.bdate_range(start = eomonth(min(closedpl['ENTRY DATE']),-1), 
                               end = eomonth(max(closedpl['ENTRY DATE']),0))

    # Create dataframe of out this date range 
    alldates = pd.DataFrame(data=daterange,columns=['DATE'])
    alldates['DATE'] = alldates['DATE'].apply(lambda d: d.date())
    
    # Create pivot tables by system out of closed P/L and open P/L
    pivot_closedpl = pd.pivot_table(closedpl, 
                                    index='DATE', 
                                    columns=closedpl['SYSTEM'], 
                                    values=['PL'], 
                                    aggfunc=sum, 
                                    fill_value=0)['PL']
    # Open P/L pivot - only if any openpl data
    if len(openpl) > 0:
        pivot_openpl = pd.pivot_table(openpl, 
                                        index='DATE', 
                                        columns=openpl['SYSTEM'], 
                                        values=['PL'], 
                                        aggfunc=sum, 
                                        fill_value=0)['PL']

    
    # SQL-style join alldates df with closedpl and openpl pivots 
    # -- outer join to get totals by system across ALL business dates; if 0 then no P/L on given date    
    pivot_closedpl = alldates.merge(pivot_closedpl, how='outer', on='DATE').fillna(0)
    pivot_openpl = alldates.merge(pivot_openpl if len(openpl) > 0 else openpl, how='outer', on='DATE').fillna(0)

    # Calculate cumulative total of closed P/L for systems
    pivot_closedpl.iloc[:,1:] = pivot_closedpl.iloc[:,1:].cumsum()
    
    # Get list of systems from closedpl after filtering it using filt_trades function
    systems = list(closedpl['SYSTEM'].unique()) if 'All' in systems else systems

    # Then set dict with capital allocations specific to these filtered systems only (SYSALLOC const contains all systems)
    # this way, when the closedpl df is added to syscapital df, only filtered systems are retained; otherwise all systems would be added back
    filtallocs = {sys:alloc for (sys,alloc) in sysallocs.items() if sys in systems}

    # Create DF that holds system capital for all dates, using system's percentage allocation of total capital
    syscapital = pd.DataFrame(filtallocs,index=alldates['DATE']).apply(lambda sysalloc: sysalloc * cap).reset_index()
    
    # Construct final equity - absolute values
    # -- add initial capital for each system at each date to its cumulative closed P/L
    # -- add open P/L as of given date to cumulative closed P/L to get final equity by system, using .add method
    # -- using iloc to only sum numerical columns (to exclude date column, otherwise error thrown), then adding back date col
    equity_abs = (pivot_closedpl.iloc[:,1:]
                  .add(syscapital.iloc[:,1:], fill_value=0)
                  .add(pivot_openpl.iloc[:,1:], fill_value=0)
                 )

    # Construct final equity - percentage return values
    # -- use absolute values to calculate daily % change 
    # -- then calculate cumualtive % return using cumprod method
    equity_pct = equity_abs.pct_change().fillna(0)
    equity_pct = ((1 + equity_pct).cumprod() - 1) * 100
    
    # Add back dates columns to both equity dfs
    equity_abs.insert(0, 'DATE', alldates['DATE'])
    equity_pct.insert(0, 'DATE', alldates['DATE'])
    
    # return both dfs - pct and abs values
    # -- abs used to calculate drawdown in ... func
    return equity_pct, equity_abs 