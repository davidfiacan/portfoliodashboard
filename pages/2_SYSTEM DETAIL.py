# Final review 12-02 - FINAL

# Non-standard libraries - batch install using 'requirements.txt' 
import pandas as pd  # pip install pandas
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
import sqlite3 # pip install pysqlite3
from sqlite3 import Error  # pip install pysqlite3
import plotly.graph_objects as go

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
st.session_state.user_pg2_plotall = False

# PAGE-SPECIFIC CONSTANTS
#-------------------------------------------------------------------

SUBHEADER = "SYSTEM DETAIL"
DASHDATE = dt.date(2022,12,31) #dt.datetime.now().date()
FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -DASHDATE.month, 
               'All' : None}

SECTIONS = {2:'System equity',
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

# INSTANTIATE DB CLASS
#-------------------------------------------------------------------

db = Db() 

# SIDEBAR (USER'S FILTERS)
#-------------------------------------------------------------------

systems = ["All"]
# systems.extend(db.do_execsql(SQLQ_SYSTEMLIST,'read')['SYSTEM'].to_list())
systems.extend(db.do_execsqlpd_r(SQLQ_SYSTEMLIST)['SYSTEM'].to_list())

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
    
# RETRIEVE DATA FROM DATABASE
#-------------------------------------------------------------------     

data_trades = db.do_execsqlpd_r(SQLQ_FILTERTRADES(f_sys, f_per),datecols=['ENTRY DATE', 'EXIT DATE'])
data_trades.columns = data_trades.columns.str.strip()
data_equity_pct, data_equity_abs = calcequity_systems(db, f_per, f_sys, SYSALLOC, CAPITAL)

# SECTION 1 (HEADER WITH METRICS) - JUST DATE FOR NOW
#-------------------------------------------------------------------

st.caption(f"Date: **{DASHDATE}**")

# SECTION 2 - SYSTEM EQUITY CHART
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

# SECTION 3 - SYSTEM CORRELATION MATRIX AND SYSTEM TRADES TABLE
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


