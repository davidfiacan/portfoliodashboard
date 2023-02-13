SQLQ_DAILYEQ = lambda cap, d_from, d_to: f"""
            -- SET PORTFOLIO'S INITIAL CAPITAL TO BASE P'S RETURN ON
            -------------------------------------------------------------
            WITH cap_p AS (SELECT {cap} AS "CAP")
            -- CALCULATE METRICS
            -------------------------------------------------------------
            SELECT 
                bd.DATE,
                -- get starting capital for portfolio
                --(SELECT "CAP" FROM cap_p) as "CAP_P",
                -- get closed, open P/L and calculate cumulative closed P/L
                --t.PL_CLOSED,
                --SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) AS "PL_CLOSED_CUM",
                --coalesce(oe.PL_OPEN,0) AS "PL_OPEN", 
                --SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) + coalesce(oe.PL_OPEN,0) AS "PL_TOTAL",
                -- Calculate cumulative equity for portfolio
                --(SELECT "CAP" FROM cap_p) + SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) + coalesce(oe.PL_OPEN,0) AS "P_EQUITY",
                -- Calculate cumulative % return for P, using cap_p
                (((SELECT "CAP" FROM cap_p) 
                    + SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) 
                    + coalesce(oe.PL_OPEN,0)) 
                    / (SELECT "CAP" FROM cap_p)
                    - 1) 
                    * 100 
                    AS "P_RET%"
            FROM bizdates as bd
            -- then join trades table for total closed P/L on business date
            LEFT JOIN (SELECT "EXIT DATE", SUM("PL") AS "PL_CLOSED"
                        FROM trades 
                        GROUP BY 1) AS t ON bd.DATE = t."EXIT DATE"
            -- then join openequity table to get open P/L on business date
            LEFT JOIN (SELECT "DATE", SUM("PL") AS "PL_OPEN"
                        FROM openequity
                        GROUP BY 1) AS oe ON bd.DATE = oe."DATE"
            WHERE bd.DATE > "{d_from}" AND bd.DATE <= "{d_to}"
            ;"""

SQLQ_DAILYEQSPY = lambda cap, d_from, d_to: f"""
                -- GET SPY'S CLOSING PRICE TO BASE SPY'S RETURN ON
                -- take 1st rowid from the filt d rng, offset by -1
                -- then join unfiltered data to get SPY's close on rowid-1
                -- this could potentially by done using explicit start date but ret null if start d weekend
                -------------------------------------------------------------
                WITH cap_spy AS (SELECT 
                                        t2."CLOSE"
                                    FROM benchmark_spy t1
                                    INNER JOIN benchmark_spy t2 ON t1.ROWID-1 = t2.ROWID
                                    WHERE t1."DATE" > "{d_from}" AND t1."DATE" <= "{d_to}"
                                    LIMIT 1),
                -- SET PORTFOLIO'S INITIAL CAPITAL TO BASE P'S RETURN ON
                -------------------------------------------------------------
                cap_p AS (SELECT {cap} AS "CAP")
                -- CALCULATE METRICS
                -------------------------------------------------------------
                SELECT 
                    bd.DATE,
                    -- get starting capital for portfolio and SPY
                    --(SELECT "CLOSE" FROM cap_spy) as "CAP_SPY",
                    --(SELECT "CAP" FROM cap_p) as "CAP_P",
                    -- get closed, open P/L and calculate cumulative closed P/L
                    --t.PL_CLOSED,
                    --SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) AS "PL_CLOSED_CUM",
                    --coalesce(oe.PL_OPEN,0) AS "PL_OPEN", 
                    --SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) + coalesce(oe.PL_OPEN,0) AS "PL_TOTAL",
                    -- Calculate cumulative equity for P and SPY
                    --(SELECT "CAP" FROM cap_p) + SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) + coalesce(oe.PL_OPEN,0) AS "P_EQUITY",
                    --b.SPY AS "SPY_EQUITY",
                    -- Calculate cumulative % return for P, using cap_p
                    (((SELECT "CAP" FROM cap_p) 
                        + SUM(t."PL_CLOSED") OVER (ROWS UNBOUNDED PRECEDING) 
                        + coalesce(oe.PL_OPEN,0)) 
                        / (SELECT "CAP" FROM cap_p)
                        - 1) 
                        * 100 
                        AS "P_RET%",
                    -- Calculate cumulative % return for SPY, using cap_spy
                    (b.SPY
                        / (SELECT "CLOSE" FROM cap_spy)
                        - 1) 
                        * 100 
                        AS "SPY_RET%"
                FROM bizdates as bd
                -- then join trades table for total closed P/L on business date
                LEFT JOIN (SELECT "EXIT DATE", SUM("PL") AS "PL_CLOSED"
                            FROM trades 
                            GROUP BY 1) AS t ON bd.DATE = t."EXIT DATE"
                -- then join openequity table to get open P/L on business date
                LEFT JOIN (SELECT "DATE", SUM("PL") AS "PL_OPEN"
                            FROM openequity
                            GROUP BY 1) AS oe ON bd.DATE = oe."DATE"
                -- then join benchmark_spy table to get SPY's closing price on business date
                LEFT JOIN (SELECT "DATE","CLOSE" AS SPY 
                            FROM benchmark_spy
                            GROUP BY 1) AS b ON bd.DATE = b."DATE"
                WHERE bd.DATE > "{d_from}" AND bd.DATE <= "{d_to}"
                ;"""

SQLQ_MONTHLY = lambda cap, d_from, d_to: f"""
            -- SET PORTFOLIO'S INITIAL CAPITAL TO BASE P'S RETURN ON
            -------------------------------------------------------------
            WITH cap_p AS (SELECT {cap} AS "CAP"),
            -- CALCULATE METRICS
            -------------------------------------------------------------
            monthly AS (SELECT 
                bd."DATE",
                -- get starting capital for portfolio and SPY
                (SELECT "CAP" FROM cap_p) as "CAP_P",
                -- get rows counts that are used for window functions to aggregate daily to monthly data
                strftime("%Y",bd."DATE") || '-' || strftime("%m",bd."DATE") AS "YYYY-MM",
                row_number() OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "DAYNO",
                count(bd."DATE") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "DAYCOUNT",
                -- get closed, open P/L and calculate cumulative closed P/L
                SUM(t."PL_CLOSED") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "PL_CLOSED_MTHLY",
                coalesce(oe.PL_OPEN,0) AS "PL_OPEN",
                SUM(t."PL_CLOSED") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) + coalesce(oe.PL_OPEN,0) AS "PL_TOTALCUM"
            FROM bizdates as bd
            -- then join trades table for total closed P/L on business date
            LEFT JOIN (SELECT "EXIT DATE", SUM("PL") AS "PL_CLOSED"
                        FROM trades 
                        GROUP BY 1) AS t ON bd.DATE = t."EXIT DATE"
            -- then join openequity table to get open P/L on business date
            LEFT JOIN (SELECT "DATE", SUM("PL") AS "PL_OPEN"
                        FROM openequity
                        GROUP BY 1) AS oe ON bd.DATE = oe."DATE"
            WHERE bd.DATE > "{d_from}" AND bd.DATE <= "{d_to}")
            -------------------------------------------------------------
            SELECT 
                "YYYY-MM",
                --SUM("PL_TOTALCUM") OVER (ROWS UNBOUNDED PRECEDING) AS "PL_TOTALCUM",
                SUM("PL_TOTALCUM") OVER (ROWS UNBOUNDED PRECEDING) / CAP_P * 100 AS "CUMRET%_P"
            FROM monthly
            WHERE "DAYNO" = "DAYCOUNT"
            ;"""

SQLQ_MONTHLYSPY = lambda cap, d_from, d_to: f"""
                -- GET SPY'S CLOSING PRICE TO BASE SPY'S RETURN ON
                -- take 1st rowid from the filt d rng, offset by -1
                -- then join unfiltered data to get SPY's close on rowid-1
                -- this could potentially by done using explicit start date but would fail if start d weekend
                -------------------------------------------------------------
                WITH cap_spy AS (SELECT 
                                        t2."CLOSE"
                                    FROM benchmark_spy t1
                                    INNER JOIN benchmark_spy t2 ON t1.ROWID-1 = t2.ROWID
                                    WHERE t1."DATE" > "{d_from}" AND t1."DATE" <= "{d_to}"
                                    LIMIT 1),
                -- SET PORTFOLIO'S INITIAL CAPITAL TO BASE P'S RETURN ON
                -------------------------------------------------------------
                cap_p AS (SELECT {cap} AS "CAP"),
                -- CALCULATE METRICS
                -------------------------------------------------------------
                monthly AS (SELECT 
                    bd."DATE",
                    -- get starting capital for portfolio and SPY
                    ----
                    (SELECT "CLOSE" FROM cap_spy) as "CAP_SPY",
                    (SELECT "CAP" FROM cap_p) as "CAP_P",
                    -- get rows counts that are used for window functions to aggregate daily to monthly data
                    ----
                    strftime("%Y",bd."DATE") || '-' || strftime("%m",bd."DATE") AS "YYYY-MM",
                    row_number() OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "DAYNO",
                    count(bd."DATE") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "DAYCOUNT",
                    -- get closed, open P/L and calculate cumulative closed P/L
                    SUM(t."PL_CLOSED") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) AS "PL_CLOSED_MTHLY",
                    coalesce(oe.PL_OPEN,0) AS "PL_OPEN",
                    SUM(t."PL_CLOSED") OVER (PARTITION BY strftime("%m",bd."DATE"), strftime("%Y",bd."DATE")) + coalesce(oe.PL_OPEN,0) AS "PL_TOTALCUM",
                    b.SPY AS "SPY_EQUITY"
                FROM bizdates as bd
                -- then join trades table for total closed P/L on business date
                LEFT JOIN (SELECT "EXIT DATE", SUM("PL") AS "PL_CLOSED"
                            FROM trades 
                            GROUP BY 1) AS t ON bd.DATE = t."EXIT DATE"
                -- then join openequity table to get open P/L on business date
                LEFT JOIN (SELECT "DATE", SUM("PL") AS "PL_OPEN"
                            FROM openequity
                            GROUP BY 1) AS oe ON bd.DATE = oe."DATE"
                -- then join benchmark_spy table to get SPY's closing price on business date
                LEFT JOIN (SELECT "DATE","CLOSE" AS SPY 
                            FROM benchmark_spy
                            GROUP BY 1) AS b ON bd.DATE = b."DATE"
                WHERE bd.DATE > "{d_from}" AND bd.DATE <= "{d_to}")
                -------------------------------------------------------------
                SELECT 
                    "YYYY-MM",
                    --SUM("PL_TOTALCUM") OVER (ROWS UNBOUNDED PRECEDING) AS "PL_TOTALCUM",
                    SUM("PL_TOTALCUM") OVER (ROWS UNBOUNDED PRECEDING) / CAP_P * 100 AS "CUMRET%_P",
                    (("SPY_EQUITY" / CAP_SPY) - 1) * 100 AS "CUMRET%_SPY"
                FROM monthly
                WHERE "DAYNO" = "DAYCOUNT"
                ;"""

SQLQ_SYSTEMLIST = """SELECT DISTINCT "SYSTEM" FROM trades;"""

SQLQ_SYSTEMEQUITY = lambda systems, d_from, d_to: f"""
                -- Subquery closed P/L, total by system and date
                WITH filtsystems_t AS 
                    (SELECT "EXIT DATE" AS "DATE", "SYSTEM", SUM("PL") AS "PL_CLOSED" FROM trades WHERE "SYSTEM" IN {systems} GROUP BY 1,2 ORDER BY 1,2),
                -- Subquery open P/L, total by system and date
                filtsystems_oe AS 
                    (SELECT "DATE", "SYSTEM", SUM("PL") AS "PL_OPEN" FROM openequity WHERE "SYSTEM" IN {systems} GROUP BY 1,2 ORDER BY 1,2)
                -- MAIN SELECT STATEMENT USING UNION
                -- this could be done by left joining bizdates with trades and then right joining trades with openequity
                -- however since SQLITE DOES NOT SUPPORT RIGHT JOINS, I had to work around this by using UNION
                -- in order to get all business dates and totals of closed and open P/L by system by business date (across all business dates, even if no /L records on given date)
                -- in order for UNION to work, all queries have to have same column count - filled with placeholder columns; these are dropped post-export in pandas
                ----------------------------------
                SELECT 
                    "DATE",
                    "SYSTEM",
                    "PL_CLOSED",
                    -- placeholders
                    "PL_OPEN", 
                    "PL_TOTAL" 
                FROM filtsystems_t t
                WHERE t."DATE" > "{d_from}" AND t."DATE" <= "{d_to}"
                -----
                UNION
                SELECT 
                    "DATE",
                    "SYSTEM",
                    -- placeholders so that biz dates with no closed P/L records are returned (to get all biz dates)
                    "PL_CLOSED",
                    "PL_OPEN", 
                    "PL_TOTAL" 
                FROM bizdates bd
                WHERE bd."DATE" > "{d_from}" AND bd."DATE" <= "{d_to}"
                -----
                UNION
                SELECT 
                    "DATE",
                    "SYSTEM",
                    "PL_CLOSED", -- placeholders
                    "PL_OPEN", 
                    "PL_TOTAL"  -- placeholders
                FROM filtsystems_oe oe
                WHERE oe."DATE" > "{d_from}" AND oe."DATE" <= "{d_to}"
                ;"""

SQLQ_FILTERTRADES = lambda s, d: f"""
                    SELECT * FROM trades 
                    WHERE SYSTEM IN {s}
                    AND [ENTRY DATE] > '{d}'
                    ORDER BY [TRADE ID] DESC
                    ;"""

SQLQ_BACKTESTSYS = """SELECT * FROM backtestequity LIMIT 1;"""

SQLQ_TOPDD = lambda s, ordby: f"""SELECT * FROM TOPDD_{s} ORDER BY {ordby} DESC;"""

SQLQ_DAILYPCT = lambda s: f"""SELECT 
                                "DATE", 
                                ("{s}" / LAG("{s}",1) OVER (ROWS 1 PRECEDING) - 1) * 100 AS "PCTCHANGE"
                            FROM backtestequity;"""
