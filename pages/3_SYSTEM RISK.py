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

def calcdd_systems(eqdata):
    """
    Calculates percentage size of drawdowns (current open loss from the highest equity value) for
        all systems (columns) in equity dataframe passed
    Returns dataframe in the same format with drawdown values
    
    params:
    -------
    eqdata: pd dataframe: dataframe containing the equity data in wide format (1 column = 1 system) 
    """
    
    for s in eqdata.columns[1:]:
        eqdata[s] = (eqdata[s] / eqdata[s].cummax() - 1) * 100 
    return eqdata

def getcurrentdd(df, s):
    """
    Gets value of current drawdown for specified trading system from the drawdown dataframe passed. 
    Returns 2 values:
     - length of current drawdown (days)
     - size of current drawdown (percent)
    
    params:
    -------
    df: pd dataframe: dataframe in wide format (1 column = 1 system) containing drawdown datapoints
    s: str: system string to get the value of the drawdown for
    """
    
    # Trim to passed sys only
    df = df.loc[:,s]
    
    # Get indices of every new drawdown; new drawdown is where 0 changes to 1 on axis 0 (using shift to determine this)
    ddindices = df[(df < 0) & (df.shift() == 0)].index

    # Determine length of last (current) drawdown by summing number of days of data starting from beginning of last DD
    ddlen = 0 if len(ddindices) == 0 else df.iloc[ddindices[-1]:].count()

    # Determine percent size of current DD 
    ddpct = df.iloc[-1]
    
    return ddlen, abs(ddpct)

def calcddcor(data_dd, rollperiod):
    """
    Calculates rolling correlation of all drawdowns of active trading systems from the dataframe passed
    Returns pd Series of combined correlation of all systems in data_dd dataframe passed
    
    params:
    -------
    data_dd: pd dataframe: dataframe in wide format (1 column = 1 system) containing drawdown datapoints
    rollperiod: int: lookback period to base the rolling correlation on
    """
    
    data_dd = data_dd.set_index('DATE')
    
    # Drop cols - systems with no DD records AND inactive systems (WPUL)
    colstodrop = [s for s in data_dd.columns if data_dd[s].sum() == 0]
    data_dd = data_dd.drop(colstodrop,axis=1).drop('WPUL',axis=1) 
    
    syscount = len(data_dd.columns)
    
    # Calculate rolling correlation of drawdowns on any given day
    rollingcor = data_dd.rolling(rollperiod).corr()
    
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
WORSTDDCOUNT = 10
DDCORLEVEL = 0.10
DDCORLOOKBACK = 60

# INSTANTIATE DB CLASS, CONNECT, DOWNLOAD & UPDATE BENCHMARK, RETRIEVE DB DATA
#-------------------------------------------------------------------

db = Db() 


# SIDEBAR (USER FILTERS)
#-------------------------------------------------------------------

# get list of unique systems from the backtest table 
# -- reading from SQL table rather than directly from CSV using pandas (10 times faster - 1 ms v 10 ms!!!)
btsystems = list(db.do_execsqlpd_r(SQLQ_BACKTESTSYS).columns[1:].unique().str.upper())

# Filters
f_per = dt.date(2000,1,1).strftime("%Y-%m-%d")
f_sys_eq = '(SELECT DISTINCT [SYSTEM] FROM trades)'

systems = ["All"]
# systems.extend(db.do_execsql(SQLQ_SYSTEMLIST,'read')['SYSTEM'].to_list())
systems.extend(db.do_execsqlpd_r(SQLQ_SYSTEMLIST)['SYSTEM'].to_list())
with st.sidebar:    
    
    # User option 1: system/s to plot in the underwater equity chart
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
#-------------------------------------------------------------------

# Equity data, drawdown data
data_equity_pct, data_equity_abs = calcequity_systems(db, f_per, f_sys_eq, SYSALLOC, CAPITAL)
data_dd = calcdd_systems(data_equity_abs)

# Rolling combined correlation of system drawdowns
ddcorr = calcddcor(data_dd, DDCORLOOKBACK)

# Get current system's drawdown figures (displayed in the key metrics section)
curddlen, curddpct = getcurrentdd(data_dd, st.session_state.user_pg3_filtsysmcdd)

# Retrieve data from database for monte-carlo DD's
data_histdd = db.do_execsqlpd_r(SQLQ_TOPDD(st.session_state.user_pg3_filtsysmcdd, st.session_state.user_pg3_mcddplot.upper()))

# Get worst DD and N-th worst DD (needed to plot the red area on the DD dist chart)
worstddn = data_histdd.head(WORSTDDCOUNT)[st.session_state.user_pg3_mcddplot.upper()].iloc[-1]
worstdd1 = data_histdd.head(1)[st.session_state.user_pg3_mcddplot.upper()].iloc[-1]

# SECTION 1 - KEY METRICS
#-------------------------------------------------------------------

st.caption(f"Date: **{DASHDATE}**")
st.subheader(SECTIONS[1](st.session_state.user_pg3_filtsysmcdd))
c1, c2, c3 = st.columns(3)
with c1:
    # Trim curddpct float so that only 2 decimal places are displayed in the metrics section
    curddpct_s = str(curddpct)
    curddpct_s = curddpct_s[:curddpct_s.find(".") + 3]
    st.caption(f"Current DD size: **{curddpct_s}**%")
with c2:
    st.caption(f"Current DD length: **{curddlen}** days")
with c3:
    st.caption("Metric 3: **bold**")
st.markdown('---')


# SECTION 2 - UNDERWATER (DRAWDOWN) CHART
#-------------------------------------------------------------------

# Determine what to plot on Y axis depending on user's filter (either all systems or only some)
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

# SECTION 3 - COMBINED DRAWDOWN CORRELATION OF ALL SYSTEMS & CURRENT DRAWDOWN DISTRIBUTION V MONTE-CARLO 
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

