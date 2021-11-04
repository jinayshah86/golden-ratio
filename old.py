import logging
import requests
from enum import Enum
import pandas as pd
from nsepy import get_history
from datetime import date, timedelta

logging.basicConfig()
log  = logging.getLogger()
log.setLevel(logging.DEBUG)


SCRIPT_NAMES = ["HDFCLIFE", "IOC"]

end = date.today()
start = end - timedelta(days=10)

def get_simple_breakout(script_name, data):
    df = []
    data["9_SMA"] = data["Close"].rolling(window=9).mean()
    data["20_SMA"] = data["Close"].rolling(window=20).mean()
    data["9_gt_20"] = data["9_SMA"] > data["20_SMA"]
    data["9_gt_20_prev"] = data["9_gt_20"].shift(1)
    data["9_gt_20_next"] = data["9_gt_20"].shift(-1)
    data["Close__prev"] = data["Close"].shift(1)
    data["Close__next"] = data["Close"].shift(-1)
    for index, row in data.iterrows():
        if row["9_gt_20"] is True and row["9_gt_20_prev"] is False:
            obj = {
                "Symbol": script_name,
                "Date": index,
                "Type": "BREAKOUT",
                "Close": row["Close"],
                "Prev Close": row["Close__prev"],
                "9 days SMA": row["9_SMA"],
                "20 days SMA": row["20_SMA"]
            }
            df.append(obj)
        elif row["9_gt_20"] is False and row["9_gt_20_prev"] is True:
            obj = {
                "Symbol": script_name,
                "Date": index,
                "Type": "BREAKDOWN",
                "Close": row["Close"],
                "Next Close": row["Close__next"],
                "9 days SMA": row["9_SMA"],
                "20 days SMA": row["20_SMA"]
            }
            df.append(obj)
    data.to_excel(f"{script_name}.xlsx")
    return pd.DataFrame(df)


def get_exponential_breakout(script_name, data):
    df = []
    data["9_EMA"] = data["Close"].ewm(span=9,adjust=False).mean()
    data["20_EMA"] = data["Close"].ewm(span=20,adjust=False).mean()
    data["9_gt_20"] = data["9_EMA"] > data["20_EMA"]
    data["9_gt_20_prev"] = data["9_gt_20"].shift(1)
    data["9_gt_20_next"] = data["9_gt_20"].shift(-1)
    data["Close__prev"] = data["Close"].shift(1)
    data["Close__next"] = data["Close"].shift(-1)
    for index, row in data.iterrows():
        if row["9_gt_20"] is True and row["9_gt_20_prev"] is False:
            obj = {
                "Symbol": script_name,
                "Date": index,
                "Type": "BREAKOUT",
                "Close": row["Close"],
                "Prev Close": row["Close__prev"],
                "9 days EMA": row["9_EMA"],
                "20 days EMA": row["20_EMA"]
            }
            df.append(obj)
        elif row["9_gt_20"] is False and row["9_gt_20_prev"] is True:
            obj = {
                "Symbol": script_name,
                "Date": index,
                "Type": "BREAKDOWN",
                "Close": row["Close"],
                "Next Close": row["Close__next"],
                "9 days EMA": row["9_EMA"],
                "20 days EMA": row["20_EMA"]
            }
            df.append(obj)
    data.to_excel(f"{script_name}.xlsx")
    return pd.DataFrame(df)


def test_nsepy():
    for script_name in SCRIPT_NAMES:
        log.debug(f"Getting history for {script_name}!!")
        data = get_history(symbol=script_name, start=start, end=end)
        log.debug(f"Got history for {script_name}!!")
        log.info(get_simple_breakout(script_name, data))
        log.info(get_exponential_breakout(script_name, data))
  
if __name__ == "__main__":
    test_nsepy()