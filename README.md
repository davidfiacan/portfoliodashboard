# Algorithmic trading systems portfolio dashboard
Dashboard made using Python, Streamlit and SQLite to provide critical performance metrics for a portfolio of algorithmic trading systems. For an in-depth overview of the project, including the dashboard functionality and technologies used, please refer to the project's video overview below

## Project's overview
[![YouTube Video](https://img.youtube.com/vi/HlqRmQXPYE8/0.jpg)](https://youtu.be/HlqRmQXPYE8)

## Live demo
Live dashboard using the example data is deployed on Render, link below
**https://portfolio-dash.onrender.com/**

## Set up instructions (if running on your own machine)
### 1) Install python on your machine
- I recommend using Anaconda distribution that can be downloaded from **https://www.anaconda.com/products/distribution**
### 2) Download the project
- save the entire project folder on your machine, e.g. `C:\tradingdashboard`
### 3) Install the required python modules
- using the command prompt, set current the working directory to the project's folder using `cd C:\tradingdashboard`
- then run `pip install -r requirements.txt` command - this will install all python modules that are required for this project, as listed in the `requirements.txt` text file
### 4) Run the data prep python script ("prep.py")
- this script creates SQL database, tables, reads data from CSV files provided (tradingdashboard\data), manipulates the data and inserts it into database, ready to be visualised & analysed from within the dashboard. It is vital that this procedure is run prior to instantiating the actual dashboard
- run the script from the command line using `python prep.py` from within the project's folder. You may need to set the current working directory to the project's folder using `cd C:\tradingdashboard`
### 5) Run the dashboard
- launch the dashboard from within the project's folder using using `streamlit run MAIN.py`. You may need to set the current working directory to the project's folder using `cd C:\tradingdashboard` if not done already
