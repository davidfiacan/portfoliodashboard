# Non-standard libraries - batch install using 'requirements.txt' 
import yfinance as yf # pip install yfinance
import pandas as pd  # pip install pandas
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3

# Standard libraries
import datetime as dt
import os
import numpy as np

# Import project's modules
from tl_commonfuncs import * 
from tl_db import * # class to interact with the database
from tl_sql_tabledefs import * # definitions of tables to be created when the db class is instantiated 
from tl_sql_queries import * # actual SQL queries stored as dictionaries


# SCRIPT-SPECIFIC CONSTANTS
#----------------------------------------------------------------------------------------

DBDIR = os.path.join(os.getcwd(),'database')
DBPATH = os.path.join(os.getcwd(),'database','db.db')
DIR_DATA = os.path.join(os.getcwd(),'data')
TODAY = dt.datetime.now().date()
F_BENCHMARK = 'SPY'
F_TRADES = 'tradelog_trades'
F_OPENEQUITY = 'tradelog_openpl'
F_BTDATA = 'backtestdata'
T_BENCHMARK = 'benchmark_spy'
T_OPENEQUITY = 'openequity'
T_TRADES = 'trades'


# SCRIPT-SPECIFIC FUNCTIONS
#----------------------------------------------------------------------------------------
    
def dnyahoodatasingle(directory, symbol, start):
    """
    Downloads stock data for a single ticker from Yahoo Finance and saves as CSV in directory folder passed
    More info see https://pypi.org/project/yfinance/
    
    params:
    -------
    directory: str: directory to save the CSV file to
    symbol: str: ticker symbol to download data for
    start: str: start date for data download in YYYY-MM-DD format    
    """
    
    # Download data from yahoo
    data = yf.Ticker(symbol).history(start=start)
    data = data.reset_index(drop=False).dropna()
    data.columns = data.columns.str.upper()
    if 'ADJ CLOSE' in data.columns:
        data = data.drop('CLOSE', axis=1)
        data = data.rename({'ADJ CLOSE':'CLOSE'})

    # Drop excessive cols
    data = data.drop(['DIVIDENDS','VOLUME','STOCK SPLITS'],axis=1)

    # Format dates as dt.date objects and add MM and YYYY columns
    data['DATE'] = data['DATE'].apply(lambda d: d.date()) 
    data.insert(len(data.columns),"MONTH",data['DATE'].apply(lambda d: d.month))
    data.insert(len(data.columns),"YEAR",data['DATE'].apply(lambda d: d.year))

    # Save as CSV / return
    data.to_csv(os.path.join(directory,f'{symbol}.csv'),index=False)
    return data

def prep_createdbdir():
    """
    Creates directory within project's folder using DBDIR constant
    This is used to store database (db file) 
    If folder exists then does nothing
    """
    
    if not os.path.isdir(DBDIR): 
        os.makedirs(DBDIR)

def prep_benchmarkdn(db):
    """
    Dashboard prep - download benchmark data from Yahoo Finance and insert into database
    
    params:
    ------
    db: instance of db class object (tl_db module)
    """
    
    messages = ["Downloading benchmark data and inserting into database"]
    printmessages(messages)
    db.do_execsqlpd_w(dnyahoodatasingle(directory = DIR_DATA, symbol = F_BENCHMARK, start = "2022-04-01"), 
                      T_BENCHMARK, 'replace')
    
    messages = ["Done!",
                '----']
    printmessages(messages)
    
def prep_openequity(db):
    """
    Dashboard prep
     - reads historical open P/L by day data saved as CSV in a wide format (saved in DIR_DATA)
     - transforms the data from original wide format to long format using pandas
     - inserts transformed data into database
    
    params:
    ------
    db: instance of db class object (tl_db module)
    """

    # Read my historical open P/L data in wide format (from my VBA trade log application I used before)
    openpl = pd.read_csv(os.path.join(DIR_DATA,f'{F_OPENEQUITY}.csv'))
    
    # Get list of unique dates in row 0 and unique columns in row 1
    dates = list(openpl.loc[0].unique())
    columns = list(openpl.loc[1].unique())
    columns.insert(0,'Date')

    # Create an empty df that will hold open equity data in long format that will be inserted into database
    openequity = pd.DataFrame(columns=columns)

    # Then re-read historical open P/L as multi-index dataframe with two headers (date and columns)
    # this is so that the df can be indexed by date (level 1)
    openpl = pd.read_csv(os.path.join(DIR_DATA,f'{F_OPENEQUITY}.csv'), header=[1,2])
    
    # Iterate over dates and populate openequity dataframe
    messages = [f"Populating open P/L by day",
               f"Start date: {str(dates[0])}",
               f"End date: {str(dates[-1])}"]
    printmessages(messages)
    
    # Iterate by date to populate data in long format
    for d in dates:

        # Loc openpl multiindex on date, drop NA rows
        dailyrecords = openpl[d].dropna(how='all')

        # Append to cumulative openequity df; this ensures that no columns are lost and are in the same order
        openequity = pd.concat([openequity,dailyrecords], ignore_index = True)

        # Fill current day's rows with current date
        openequity.loc[len(openequity)-len(dailyrecords):,'Date'] = d

    # Original data contains duplicate days, resulting in duplicate columns of openpl multi-index df
    # these are marked with ".1" suffices by pandas - below gets a list of all these duplicated columns
    # which are then removed from the openequity df
    # example openpl['01/02/2021']
    dropcols = [c for c in openequity.columns if c.find(".") != - 1]
    openequity = openequity.drop(columns=dropcols,axis=1)

    # Fill N/A with 0's so that the values can be added together
    openequity = openequity.fillna(0)

    # Final formatting - rename columns, drop excessive columns, capitalise columns names
    openequity = (openequity.rename({'Exit price':'CLOSING PRICE','P/L':'PL','L/S':'ACTION'}, axis=1)
                              .drop(['Allocation'], axis=1))
    openequity.columns = openequity.columns.str.upper()

    # Fill action columns with appropriate marker (L = long, buying the stock; S = Short, short-selling the stock)
    # openpl df did not carry this information at the start, so some earlier records were N/As and replaced with 0's
    openequity['ACTION'] = openequity['ACTION'].replace(0,'L')
    
    # Convert date column to date (currently string)
    openequity['DATE'] = pd.to_datetime(openequity['DATE'], dayfirst=True)
    openequity['DATE'] = openequity['DATE'].apply(lambda d: d.date())
    
    # Write to open equity table
    messages = [f"Writing {str(len(openequity))} open P/L by day records to database"]
    printmessages(messages)
    db.do_execsqlpd_w(openequity,T_OPENEQUITY,'replace')

    messages = ["Done!",
                '----']
    printmessages(messages)
    
def prep_trades(db):
    """
    Dashboard prep 
     - reads live, executed trades from CSV (saved in DIR_DATA)
     - inserts into database 
    
    params:
    ------
    db: instance of db class object (tl_db module)
    """
    
    messages = ["Reading trades data"]
    printmessages(messages)
    
    # trades = pd.read_csv(os.path.join(DIR_DATA,f'{F_TRADES}.csv'))
    trades = pd.read_csv(os.path.join(DIR_DATA,f'{F_TRADES}.csv'),parse_dates=['Entry date','Exit date'],dayfirst=True)
    
    # Convert dates columns to correct format so that dates can be filtered directly in SQlite
    trades['Entry date'] = trades['Entry date'].apply(lambda d: d.date())
    trades['Exit date'] = trades['Exit date'].apply(lambda d: d.date())
    
    # Format columns - capitalise and remove whitespaces
    trades.columns = trades.columns.str.strip().str.upper()
    
    messages = [f"Writing {len(trades)} executed trade records into database"]
    printmessages(messages)
    
    db.do_execsqlpd_w(trades,T_TRADES,'replace')
    
    messages = ["Done!",
            '----']
    printmessages(messages)

def prep_backtestdata(db):
    """
    Dashboard prep
     - reads backtest (hypothetical) data for all trading systems from CSV (saved in DIR_DATA)
     - inserts into database - creates 'backtestequity' table with dynamic number of columns (1 column = 1 system)
         depending on how many columns appear in the CSV (scalable solution as column count may change)
    
    params:
    ------
    db: instance of db class object (tl_db module)
    """

    messages = ["Reading backtest data"]
    printmessages(messages)
    
    # Read backtest data 
    btdata = pd.read_csv(os.path.join(DIR_DATA,f'{F_BTDATA}.csv'),parse_dates=['Date'], dayfirst=True)
    btdata['Date'] = btdata['Date'].apply(lambda d: d.date())
    btdata.columns = btdata.columns.str.upper()
    systems = btdata.columns[1:]
    
    messages = [f"Writing backtest data to database for systems {str(list(systems))}"]
    printmessages(messages)
    
    # Create backtestequity table
    [db.do_execsql(q,'write') for q in TABLE_BACKTEST]

    # Add dynamic number of columns (1 col per system) to match systems in btdata df
    [db.do_execsql(TABLE_BACKTEST2(sys),'write') for sys in systems]
    
    # Then insert data to table from df using pandas 
    db.do_execsqlpd_w(btdata,'backtestequity',if_exists='replace',dtype='real')
    
    messages = ["Done!", 
                '----']
    printmessages(messages)
    
def prep_montecarlo(db, runscount):
    """
    Dashboard prep
     - runs a simplified version of monte-carlo (MC) simulation on the backtest data previouly inserted into database
     - MC simulation takes daily percent changes in backtest equity curve of a system and reshuffles these randomly N times
     - rationale of this process is to introduce randomness to stress-test the trading system's backtest (hypothetical) performance
     - demonstrating working with numpy arrays (faster than pandas), transforming data from wide format to long format
    
    params:
    ------
    runscount: int: how many monte-carlo runs to be conducted
    db: instance of db class object (tl_db module)
    """
    
    messages = [f"Running monte-carlo simulation ({str(runscount)} runs) with backtest data"]
    printmessages(messages)
    
    btdata = db.do_execsqlpd_r("SELECT * FROM backtestequity").drop('DATE',axis=1)
    systems = list(btdata.columns)
    
    eqdata_wide, eqdata_long = {}, {}
    
    # Iterate through all systems (columns) in backtestequity table
    for s in systems:
        startcap = btdata.loc[0,s] # Use first row as starting capital to base the calcs on
        
        # Calculate daily % change in equity & convert to numpy array (faster)
        equitychanges = btdata[s].pct_change().fillna(0).to_numpy()
        
        # Reshape the array to 1 column (so that it can be concatenated column-wise)
        equitychanges = equitychanges.reshape((len(equitychanges),1))

        # Create monte-carlo np array that will hold all monte-carlo runs
        mc = np.array(equitychanges)
        
        # Using while loop as slightly faster than for over range(1,runscount)
        r = 1
        while r <= runscount: # Iterate N times
            r += 1 # increment at each run

            # Shuffle equitychanges np array in place
            np.random.shuffle(equitychanges)

            # Concatenate existing mc array with shuffled equitychanges, column-wise
            mc = np.concatenate((mc, equitychanges),axis=1)

        # Final mc equities - convert to pd df & calc cum product of mc equity changes multiplied by initial capital
        mcdf = pd.DataFrame(mc)
        for c in mcdf.columns:
            mcdf[c] = np.cumprod(1 + mcdf[c].values) * startcap
              
        # Add data to dictionaries
        eqdata_wide[s] = mcdf
        eqdata_long[s] = mcdf.melt(var_name="EQNUM",value_name="EQVAL") # long format - one column that holds ALL MC equities
    
    datasize = len(btdata) * len(systems) * runscount
    messages = [f"Writing {str(datasize)} monte-carlo datapoints to database ({str(len(btdata))} rows * {str(len(systems))} * {str(runscount)} runs)"]
    printmessages(messages)
    
    # Once done, re-create monte-carlo tables for all systems in mceq_wide
    [db.do_execsql(q(sys),'write') for q in TABLE_MCEQUITIES for sys in eqdata_wide.keys()]
    
    # Then insert new monte-carlo data to database for each system
    [db.do_execsqlpd_w(eqdata_long[sys], f"MCEQUITY_{sys}", "replace", dtype = 'float') for sys in eqdata_wide.keys()]
    
    messages = ["Creating top monte-carlo drawdown tables for all systems"]
    printmessages(messages)
    
    # 2 sets of tables for each system in mceq_wide
    [db.do_execsql(q(s),'write') for q in TABLE_MCTOPDD for s in eqdata_wide.keys()]
    
    messages = ["Done!",
                '----']
    printmessages(messages)
    # return eqdata_wide, eqdata_long
    
def prep_bizdates(db):
    """
    Dashboard prep 
     - creates 'bizdates' table by extracting a list of buiness days from 'benchmark_spy' table 
    """
    
    messages = ["Extracting list of business dates"]
    printmessages(messages)
    
    [db.conn.cursor().execute(sql) for sql in TABLE_BIZDATES]
    
    messages = ["Writing into bizdates table"]
    printmessages(messages)
    
    messages = ["Done!",
            '----']
    printmessages(messages)
    
    
# MAIN
#----------------------------------------------------------------------------------------
        
if __name__ == '__main__':
    
    messages = []
    messages = ["TRADING DASHBOARD DATA PREP",
                "------------",
                f"Date: {str(TODAY)}",
               "------------"]
    printmessages(messages)
    
    # Create database folder within the project's directory 
    prep_createdbdir()
    
    # Instantiate Db class from tl_db module (will create database in the folder created by prep_createdbdir func)
    messages = [f"Connecting to database: {DBPATH}"]
    printmessages(messages)
    db = Db()
    
    # Run individual prep subroutines to insert data into SQL database
    prep_benchmarkdn(db)
    prep_openequity(db)
    prep_trades(db)
    prep_backtestdata(db)
    prep_montecarlo(db, runscount = 250)
    prep_bizdates(db)
    
    
