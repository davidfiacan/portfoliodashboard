# 22/01
# sort out charts' legends - get rid of them
# DD rolling correl - add simple moving average to the chart as well
# finish making proper docstrings across all pages
# then final clean up, restructuring, getting rid of obsolote functions etc.
# then done!!!!!


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

def calcdd_systems(eqdata):
    for s in eqdata.columns[1:]:
        eqdata[s] = (eqdata[s] / eqdata[s].cummax() - 1) * 100 
    return eqdata

def getcurrentdd(df, s):
    
    # Trim to passed sys only
    df = df.loc[:,s]
    
    # Get indices of every new drawdown; new drawdown is where 0 changes to 1 on axis 0 (using shift to determine this)
    ddindices = df[(df < 0) & (df.shift() == 0)].index

    # Determine length of last (current) drawdown by summing number of days of data starting from beginning of last DD
    ddlen = 0 if len(ddindices) == 0 else df.iloc[ddindices[-1]:].count()

    # Determine percent size of current DD 
    ddpct = df.iloc[-1]
    
    return ddlen, abs(ddpct)

def calcddcor(data_dd, mawindow):
    
    data_dd = data_dd.set_index('DATE')
    
    # Drop cols - systems with no DD records AND inactive systems (WPUL)
    colstodrop = [s for s in data_dd.columns if data_dd[s].sum() == 0]
    data_dd = data_dd.drop(colstodrop,axis=1).drop('WPUL',axis=1) 
    
    syscount = len(data_dd.columns)
    
    # Calculate rolling correlation of drawdowns on any given day
    rollingcor = data_dd.rolling(mawindow).corr()
    
    # Then work out combined correlation 
    # -- need to remove 1's from the correlation matrix, remove duplicates and then work out the overall average
    combinedcor = (rollingcor.sum(axis=1).groupby(level=0).sum()-syscount)/2/syscount

    return combinedcor  
    

# PAGE CONFIG
#-------------------------------------------------------------------
st.set_page_config(page_title="Trading dashboard 1.0", page_icon=":bar_chart:", layout="wide")
st.session_state.user_pg3_plotall = False


# PAGE-SPECIFIC CONSTANTS
#-------------------------------------------------------------------
SUBHEADER = "SYSTEM RISK"

# DASHDATE = dt.datetime.now().date()
DASHDATE = dt.date(2022,12,31)

FILTERS_PER = {'Current month' : -1, 
               '2 mths' : -2, 
               '3 mths': -3, 
               'YTD': -DASHDATE.month, 
               'All' : None}

SECTIONS = {1:lambda s: f'Key metrics for {s}',
            2:'System underwater equity',
            3:'Rolling drawdown correlation',
            4:'Monte-carlo drawdown distribution'            
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
DIR_DATA = os.path.join(os.getcwd(),'data')
F_BTDATA = 'backtestdata'
WORSTDDCOUNT = 10
DDCORLEVEL = 0.10
DDCORLOOKBACK = 60

# INSTANTIATE DB CLASS, CONNECT, DOWNLOAD & UPDATE BENCHMARK, RETRIEVE DB DATA
#-------------------------------------------------------------------
db = Db() # tl_db.Db class



# Sidebar (user filters)
#-------------------------------------------------------------------

# get list of unique systems from the backtest table 
# -- reading from SQL table rather than directly from CSV using pandas (1 ms v 10 ms!!!)
btsystems = list(db.do_execsqlpd_r(SQLQ_BACKTESTSYS).columns[1:].unique().str.upper())

# Filtes for 
f_per = dt.date(2000,1,1).strftime("%Y-%m-%d")
f_sys_eq = '(SELECT DISTINCT [SYSTEM] FROM trades)'

systems = ["All"]
systems.extend(db.do_execsql(SQLQ_SYSTEMLIST,'read')['SYSTEM'].to_list())

with st.sidebar:    
    
    # User option 1: 
    st.multiselect("Systems for underwater equity plot", 
               systems, 
               key = "user_pg3_filtsys", 
               default = ["All"])
    if "All" in st.session_state.user_pg3_filtsys:# and len(st.session_state.user_pg2_systems) > 1:
        st.session_state.user_pg3_plotall = True
        st.caption("**All** in multi-select, plotting all systems")
    
    # User option 2: drawdown distribution (current DD v N worst monte-carlo DDs) single system selection
    st.sidebar.selectbox("System for drawdown distribution", 
                         options = btsystems, 
                         key = "user_pg3_filtsysmcdd", 
                         index = 0)
    
    # User option 3: drawdown distribution calculation basis
    st.radio("DD distribution calculation basis", 
             ('Pct', 'Days'), 
             key = "user_pg3_mcddplot", 
             label_visibility = 'visible', 
             horizontal=True)
    
# RETRIEVE DATA FROM DATABASE
# using QUERIES dict that holds SQL queries (in 'tl_sql_queries' script)
#-------------------------------------------------------------------

# Retrieve equity data and calculate drawdowns 
data_equity_pct, data_equity_abs = calcequity_systems(f_per, f_sys_eq, SYSALLOC, CAPITAL)
data_dd = calcdd_systems(data_equity_abs)

# Calculate rolling drawdown correlation
ddcorr = calcddcor(data_dd, DDCORLOOKBACK)

# Get current system's drawdown figures (displayed in the key metrics section)
curddlen, curddpct = getcurrentdd(data_dd, st.session_state.user_pg3_filtsysmcdd)

# Retrieve data from database for monte-carlo DD's
data_histdd = db.do_execsqlpd_r(SQLQ_TOPDD(st.session_state.user_pg3_filtsysmcdd, st.session_state.user_pg3_mcddplot.upper()))

# Get worst DD and N-th worst DD (needed to plot the red area on the DD dist chart)
worstddn = data_histdd.head(WORSTDDCOUNT)[st.session_state.user_pg3_mcddplot.upper()].iloc[-1]
worstdd1 = data_histdd.head(1)[st.session_state.user_pg3_mcddplot.upper()].iloc[-1]

# Section 1 - header with metrics
#-------------------------------------------------------------------
st.caption(f"Date: **{DASHDATE}**")
st.subheader(SECTIONS[1](st.session_state.user_pg3_filtsysmcdd))
c1, c2, c3 = st.columns(3)
with c1:
    # Format curddpct to 2 decimal places (so that the text in key metrics section is pretty-formatted)
    curddpct_s = str(curddpct)
    curddpct_s = curddpct_s[:curddpct_s.find(".") + 3]
    st.caption(f"Current DD size: **{curddpct_s}**%")
with c2:
    st.caption(f"Current DD length: **{curddlen}** days")
with c3:
    st.caption("Metric 3: **bold**")
st.markdown('---')


# Section 2 - system underwater equity 
#-------------------------------------------------------------------

# Determine what to plot on Y axis depending on user's filter
yplot =  data_dd.columns[1:] if st.session_state.user_pg3_plotall else st.session_state.user_pg3_filtsys

st.subheader(SECTIONS[2])
fig = px.line(data_dd, 
              x = data_dd['DATE'], 
              y = yplot,
              color_discrete_sequence=px.colors.qualitative.Light24)
fig.update_yaxes(title=None)
fig.update_xaxes(title=None)
fig.update_layout(legend_orientation='h')
st.plotly_chart(fig, use_container_width=True)

# Section 3 - system drawdown correlation & drawdown distribution
#-------------------------------------------------------------------
c1, c2 = st.columns(2)

# Combined correlation of all system drawdowns
with c1:
    st.subheader(SECTIONS[3])
    st.caption(f"Combined {str(DDCORLOOKBACK)}-day rolling correlation of active system drawdowns")
    fig = px.line(ddcorr[DDCORLOOKBACK:])
    fig.add_hline(y=DDCORLEVEL, line_width=1, line_dash="dot", line_color="red" )
    fig.update_yaxes(title=None)
    fig.update_xaxes(title=None)
    fig.update_layout(legend_orientation='h')
    st.plotly_chart(fig,use_container_width=True)
    
# Current DD v MC dd's distribution chart
with c2:
    st.subheader(SECTIONS[4])
    st.caption(f"Current drawdown (yellow) compared to worst {str(WORSTDDCOUNT)} monte-carlo drawdowns (red area)")
    
    fig = px.scatter(data_histdd,x=[st.session_state.user_pg3_mcddplot.upper()])
    # Add vertical markers - current DD 
    curplot = curddlen if st.session_state.user_pg3_mcddplot == 'Days' else curddpct
    fig.add_vline(x=curplot, line_width=1, line_dash="dot", line_color="yellow" )
    fig.add_vrect(x0=worstddn, x1=worstdd1, line_width=0, fillcolor="red", opacity=0.3)
    fig.update_xaxes(title=f"MC DD {st.session_state.user_pg3_mcddplot.upper()}")
    fig.update_yaxes(title='Count', visible=False)
    fig.update_layout(legend_orientation='h')
    st.plotly_chart(fig,use_container_width=True)

