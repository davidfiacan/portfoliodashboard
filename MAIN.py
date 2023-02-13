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
import os

# Import project's modules
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

SUBHEADER = "PORTFOLIO"
DASHDATE = dt.date(2022,12,31) #dt.datetime.now().date()
FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -DASHDATE.month, 
               'All' : None}
SECTIONS = {1:'Portfolio equity',
            2:'Monthly P/L'
           }
CAPITAL = 80_000

# INSTANTIATE DB CLASS
#-------------------------------------------------------------------

db = Db() 

# SIDEBAR (USER'S FILTERS)
#-------------------------------------------------------------------

with st.sidebar:    
    st.sidebar.selectbox("Equity and monthly table date range", 
                         options = FILTERS_PER.keys(), 
                         key = "user_main_filtper", 
                         index = 3)
    st.sidebar.checkbox("Benchmark comparison", 
                        value = False, 
                        key = "user_main_benchmark")
    
    # Set date parameter for SQL query based on user's selection
    if st.session_state.user_main_filtper == 'All':
        f_per = dt.date(2000,1,1)
    else:
        f_per = eomonth(DASHDATE,FILTERS_PER[st.session_state.user_main_filtper])
    f_per = f_per.strftime("%Y-%m-%d")
    

# RETRIEVE DATA FROM DATABASE
#-------------------------------------------------------------------

if st.session_state.user_main_benchmark:
    data_equity = db.do_execsqlpd_r(SQLQ_DAILYEQSPY(CAPITAL, f_per, DASHDATE), datecols=['DATE'])
    data_monthly = db.do_execsqlpd_r(SQLQ_MONTHLYSPY(CAPITAL, f_per, DASHDATE))
else:
    data_equity = db.do_execsqlpd_r(SQLQ_DAILYEQ(CAPITAL, f_per, DASHDATE), datecols=['DATE'])
    data_monthly = db.do_execsqlpd_r(SQLQ_MONTHLY(CAPITAL, f_per, DASHDATE))
    

# SECTION 1 - PORTFOLIO EQUITY CHART
#-------------------------------------------------------------------

st.caption(f"Date: **{DASHDATE}**")
st.subheader(SECTIONS[1])
fig = px.line(data_equity, 
              x = 'DATE', 
              y = list(data_equity.columns)[1:],
              color_discrete_sequence=["aqua","darkgreen"])
fig.update_yaxes(title=None)
fig.update_xaxes(title=None)
fig.update_layout(legend_orientation='h')
st.plotly_chart(fig, use_container_width=True)


# SECTION 2 - MONTHLY PROFIT TABLE AND CHART
#-------------------------------------------------------------------

col1, col2 = st.columns(2)
with col1:
    st.subheader(SECTIONS[2])
    # Format all but first column to two decimals
    st.dataframe(data_monthly.style.format(subset=data_monthly.columns[1:], 
                                           formatter="{:.2f}"))
    gencsvdownload(data_monthly, "monthly pl data")
    
with col2:
    fig = px.bar(data_monthly, 
              x = 'YYYY-MM', 
              y = list(data_monthly.columns)[1:],
                 color_discrete_sequence=["aqua","darkgreen"])
    fig.update_yaxes(title=None)
    fig.update_xaxes(title=None)
    fig.update_layout(legend_orientation='h')
    st.plotly_chart(fig, use_container_width=True)
    