
# this table definition is a list because it consists of multiple statements
# -- sqlite can only execute one statement at the time,
# -- solved by iterating over statements in this list and executing one at a time
TABLE_BIZDATES = ["DROP TABLE IF EXISTS bizdates;",
                  """-- create business dates table using dates from downloaded benchmark_spy data
                  -- AFTER benchmark_spy table was created and data downloaded
                  -- starting from 2020, not everything
                  -------------------------------------
                  CREATE TABLE bizdates
                  AS 
                      SELECT "DATE" from benchmark_spy
                      WHERE "DATE" >= "2022-01-01";""",
                  """-- Add month and year columns to carry MM and YYYY integers
                      -- this could have been done at table creating as benchmark SPY has the same info,
                      -- but wanted to demonstrate how this is done the 'harder' way
                     -------------------------------------
                    ALTER TABLE bizdates
                        ADD "MONTH" INT;""",
                  """ALTER TABLE bizdates
                        ADD "YEAR" INT;""",
                  """-- Fill month and year cols w/MM and YYYY integers
                    UPDATE bizdates
                    SET 
                        "MONTH" = strftime("%m","DATE"),
                        "YEAR" = strftime("%Y","DATE");"""
                 ]

                  
# Trades - holds all trades, both closed and open
TABLE_TRADESALL = """ CREATE TABLE IF NOT EXISTS trades (
                                    [TRADE ID] integer PRIMARY KEY,
                                    SYSTEM text,
                                    ACTION text,
                                    TICKER text,
                                    [ENTRY DATE] text,
                                    [ENTRY PRICE] real, 
                                    QUANTITY integer,
                                    COMMISSION real,
                                    [EXIT DATE] text,
                                    [EXIT PRICE] real,
                                    PL real
                                ); """

TABLE_BENCHMARK = """ CREATE TABLE IF NOT EXISTS benchmark_spy (
                                            DATE text PRIMARY KEY,
                                            OPEN REAL,
                                            HIGH REAL,
                                            LOW REAL,
                                            CLOSE REAL,
                                            MONTH INT,
                                            YEAR INT
					);"""

TABLE_OPENEQUITY = """CREATE TABLE IF NOT EXISTS openequity (
                      "DATE" TEXT , 
                      "SYSTEM" TEXT,
                      "ACTION" TEXT
                      "TICKER" TEXT,
                      "ENTRY PRICE" TEXT,
                      "QTY" INT,
                      "CLOSING PRICE" REAL,
                      "PL " REAL
                    );"""

TABLE_BACKTEST = ["DROP TABLE IF EXISTS backtestequity;",
                  """CREATE TABLE backtestequity (
                              "DATE" text PRIMARY KEY
                  );"""
                  ]

TABLE_BACKTEST2 = lambda s: f"""ALTER TABLE backtestequity ADD "{s}" REAL;"""

TABLE_MCEQUITIES = [lambda s: f"DROP TABLE IF EXISTS MCEQUITY_{s};",
                   lambda s: f"CREATE TABLE MCEQUITY_{s} ('EQUITY' REAL);"]
                    
TABLE_MCTOPDD = [lambda s: f"DROP TABLE IF EXISTS TOPDD_{s};"
                 ,lambda s: f"""CREATE TABLE TOPDD_{s} AS
                                    -- Subquery 1: calculate highest high of each individual monte carlo equity curve for given system
                                    -----------------------------------------------------
                                    WITH equityhigh AS (SELECT 
                                                        "EQNUM",
                                                        "EQVAL",
                                                        MAX("EQVAL") OVER (PARTITION BY "EQNUM" ROWS UNBOUNDED PRECEDING) AS "EQUITY_HIGH"
                                                    FROM MCEQUITY_{s}
                                                    )
                                    -- Subquery 2: calculate drawdown days and percentages
                                    -----------------------------------------------------
                                    , drawdowns AS (SELECT 
                                                        "EQNUM", 
                                                        "EQVAL",
                                                        "EQUITY_HIGH",
                                                        CASE 
                                                            WHEN "EQVAL" < "EQUITY_HIGH"
                                                            THEN 1
                                                            ELSE 0
                                                        END AS "DD",
                                                        CASE 
                                                            WHEN "EQVAL" < "EQUITY_HIGH"
                                                            THEN ABS(("EQVAL" / "EQUITY_HIGH" - 1) * 100)
                                                            ELSE 0
                                                        END AS "DD_%"
                                                    FROM equityhigh
                                                    )
                                    -- Main select statement
                                    -----------------------------------------------------
                                    SELECT  
                                        CAST("EQNUM" AS INT) AS "MCEQUITYNUM"
                                        --"EQVAL",
                                        ,"EQUITY_HIGH"
                                        --"DD",
                                        --"DD_%"
                                        ,MAX("DD_%") AS "PCT"
                                        ,SUM("DD") AS "DAYS"
                                    FROM drawdowns
                                    WHERE
                                        "DD" = 1 -- this is to get 1 record per DD only 
                                    GROUP BY "EQUITY_HIGH" -- this value should be unique at each DD (technically can be same as other but highly unlikely so will do for now) 
                                    ORDER BY "PCT" DESC
                                    ;"""
                ]

