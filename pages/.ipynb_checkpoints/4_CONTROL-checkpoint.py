import pandas as pd  # pip install pandas
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3
import datetime as dt
import numpy as np
import os

# Import modules
from tl_commonfuncs import * 
from tl_db import * # class to interact with the database
from tl_sql_tabledefs import * # definitions of tables to be created when the db class is instantiated 
from tl_sql_queries import * # actual SQL queries stored as dictionaries




# PAGE-SPECIFIC FUNCTIONS
#-------------------------------------------------------------------






# PAGE CONFIG
#-------------------------------------------------------------------
st.set_page_config(page_title="Trading dashboard 1.0", page_icon=":bar_chart:", layout="wide")


# PAGE-SPECIFIC CONSTANTS
#-------------------------------------------------------------------
SUBHEADER = "CONTROL"
TODAY = dt.datetime.now().date()
FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -TODAY.month, 
               'All' : None}
SECTIONS = {1:'Key metrics', 
            2:'Portfolio equity',
            3:'Monthly P/L',
            4:'Trades'
           }
CAPITAL = 80_000



# INSTANTIATE DB CLASS, CONNECT, DOWNLOAD & UPDATE BENCHMARK, RETRIEVE DB DATA
#-------------------------------------------------------------------

db = Db() # tl_db.Db class

# Read sample trades data and insert into database
trades = pd.read_csv(os.path.join(os.getcwd(),'data','tradelog.csv'))
trades.columns = trades.columns.str.upper()
db.do_execsqlpd_w(trades,'trades','replace')

# Retreive data from database - using QUERIES dict that holds SQL queries (in 'tl_sql_queries' script)
tldata = db.do_execsqlpd_r(QUERIES_MAIN['trades'], datecols=['ENTRY DATE','EXIT DATE'])



# Sidebar (user filters)
#-------------------------------------------------------------------

systems = ['All']
systems.extend(list(tldata['SYSTEM'].str.strip().unique()))

with st.sidebar:    
    st.caption("Date range for equity and trades table")
    st.sidebar.selectbox(f"Dates", options = FILTERS_PER.keys(), key = "user_main_filtper", index = 3)
    st.caption("Filter trades table")
    st.sidebar.selectbox("System", options = systems, key = "user_main_filtsys", index = 0)
    st.sidebar.checkbox("Benchmark comparison", value = True, key = "user_main_benchmark")



# Section 1 - header with metrics
#-------------------------------------------------------------------
# st.subheader(SECTIONS[1])
# st.caption(f"Date: **{TODAY}**")
# c1, c2, c3 = st.columns(3)
# with c1:
#     st.caption("Metric 1: **bold**")
# with c2:
#     st.caption("Metric 2: **bold**")
# with c3:
#     st.caption("Metric 3: **bold**")
# st.markdown('---')