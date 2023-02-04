# 12/01/2023
# FINAL


import pandas as pd  # pip install pandas
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3
import datetime as dt
import numpy as np
import os

import plotly.graph_objects as go

# Import modules
from tl_commonfuncs import * 
from tl_db import * # class to interact with the database
from tl_sql_tabledefs import * # definitions of tables to be created when the db class is instantiated 
from tl_sql_queries import * # actual SQL queries stored as dictionaries


# PAGE-SPECIFIC FUNCTIONS
#-------------------------------------------------------------------

def calcequity_systems(per, systems, sysallocs, capital): 
        
    # Execute SQL query - closed and open P/L by business date by system
    df = db.do_execsqlpd_r(SQLQ_SYSTEMEQUITY(systems,per,DASHDATE), datecols=['DATE'])
    
    # Clean up data after SQL query; drop excessive cols and rows, replace placeholder values with 0'
    df.columns = df.columns.str.replace('"','')
    
    # Using new pandas method chaining
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


# PAGE CONFIG
#-------------------------------------------------------------------
st.set_page_config(page_title="Trading dashboard 1.0", page_icon=":bar_chart:", layout="wide")
st.session_state.user_pg2_plotall = False

# PAGE-SPECIFIC CONSTANTS
#-------------------------------------------------------------------
SUBHEADER = "SYSTEM DETAIL"
# DASHDATE = dt.datetime.now().date()
DASHDATE = dt.date(2022,12,31) #dt.datetime.now().date()
FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -DASHDATE.month, 
               'All' : None}

SECTIONS = {1:'Key metrics',
            2:'System equity',
            3:'System correlation matrix',
            4:'Trades'
           }

SYSALLOC = {'MPL':0.33,
         'WPUL':0.33,
         'DRAA':0.33,
         'TMR':0.33,
         'MRIDV3L':0.33,
         'MRIDV3S':0.33,
         'MICROREV':0.33,
         'FREV1':0.25, 
         'MES_NDAY':0.25,
         'MNQ_BO1':0.25,
         'MNQ_BO4L':0.25,
         'MNQ_BO3S':0.25,
         'MES_BOLLINGER':0.25}
CAPITAL = 80_000

# INSTANTIATE DB CLASS, CONNECT, DOWNLOAD & UPDATE BENCHMARK, RETRIEVE DB DATA
#-------------------------------------------------------------------
db = Db() # tl_db.Db class

# Sidebar (user filters)
#-------------------------------------------------------------------
systems = ["All"]
systems.extend(db.do_execsql(SQLQ_SYSTEMLIST,'read')['SYSTEM'].to_list())
with st.sidebar:    
    st.sidebar.selectbox("Dates", 
                         options = FILTERS_PER.keys(), 
                         key = "user_pg2_filtper", 
                         index = 3)
    st.multiselect("Systems", 
                   systems, 
                   key = "user_pg2_systems", 
                   default = ["All"])
    if "All" in st.session_state.user_pg2_systems:# and len(st.session_state.user_pg2_systems) > 1:
        st.session_state.user_pg2_plotall = True
        st.caption("**All** in multi-select, plotting all systems")

    # Set date parameter for SQL query based on user's selection
    if st.session_state.user_pg2_filtper == "All":
        f_per = dt.date(2000,1,1)
    else:
        f_per = eomonth(DASHDATE,FILTERS_PER[st.session_state.user_pg2_filtper])
    f_per = f_per.strftime("%Y-%m-%d")
    
    # Set system parameter for SQL query based on user's selection
    if st.session_state.user_pg2_plotall:
        f_sys = '(SELECT DISTINCT [SYSTEM] FROM trades)'
    else:
        f_sys = str(st.session_state.user_pg2_systems).replace("[","(").replace("]",")")
    # st.caption(f_sys)

# Section 1 - header with metrics
#-------------------------------------------------------------------
st.caption(f"Date: **{DASHDATE}**")
# st.subheader(SECTIONS[1])
# c1, c2, c3 = st.columns(3)
# with c1:
#     st.caption("Metric 1: **bold**")
# with c2:
#     st.caption("Metric 2: **bold**")
# with c3:
#     st.caption("Metric 3: **bold**")
# st.markdown('---')

        
# RETRIEVE DATA FROM DATABASE
# using QUERIES dict that holds SQL queries (in 'tl_sql_queries' script)
#-------------------------------------------------------------------        
data_trades = db.do_execsqlpd_r(SQLQ_FILTERTRADES(f_sys, f_per),datecols=['ENTRY DATE', 'EXIT DATE'])
data_trades.columns = data_trades.columns.str.strip()
data_equity_pct, data_equity_abs = calcequity_systems(f_per, f_sys, SYSALLOC, CAPITAL)

# Section 2 - system equity chart
#-------------------------------------------------------------------

# Determine which columns to plot on Y axis; depending on whether all systems picked
try:
    data_equity_pct = data_equity_pct.drop(['PL','SYSTEM'],axis=1)
except:
    pass
yplot =  data_equity_pct.columns[1:] if st.session_state.user_pg2_plotall else st.session_state.user_pg2_systems
    
st.subheader(SECTIONS[2])
fig = px.line(data_equity_pct, 
              x = data_equity_pct['DATE'], 
              y = yplot,
              color_discrete_sequence=px.colors.qualitative.Light24)
fig.update_yaxes(title=None)
fig.update_xaxes(title=None)
fig.update_layout(legend_orientation='h')
st.plotly_chart(fig, use_container_width=True)

# Section 3 - system correlation matrix (c1) and trades table (c2)
#-------------------------------------------------------------------

data_equity_corr = data_equity_abs.drop('DATE',axis=1).corr()
c1, c2 = st.columns(2)
with c1:
    st.subheader(SECTIONS[3])
    fig = go.Figure()
    fig.add_trace(
        go.Heatmap(
            x = data_equity_corr.columns,
            y = data_equity_corr.index,
            z = np.array(data_equity_corr),
            text=data_equity_corr.values,
            texttemplate='%{text:.2f}',
            colorscale = 'temps'
        )
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader(SECTIONS[4])
    st.dataframe(data_trades.reset_index(drop=True))
    gencsvdownload(data_trades, "trades data")


