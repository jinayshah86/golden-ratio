import logging
import pickle
from enum import Enum
from datetime import date, timedelta, datetime
from pprint import pprint

import pandas as pd
import requests

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class YahooFinance():
    class URLS(str, Enum):
        history = "https://yfapi.net/v8/finance/spark?interval={interval}&range={range}&symbols={symbols}"

    class INTERVAL(str, Enum):
        ONE_MINUTE = "1m"
        FIVE_MINUTES = "5m"
        FIFTEEN_MINUTES = "15m"
        ONE_DAY = "1d"
        ONE_WEEK = "1wk"
        ONE_MONTH = "1mo"

    class RANGE(str, Enum):
        ONE_DAY = "1d"
        FIVE_DAYS = "5d"
        ONE_WEEK = "1wk"
        ONE_MONTH = "1mo"
        THREE_MONTHS = "3mo"
        SIX_MONTHS = "6mo"
        ONE_YEAR = "1y"
        FIVE_YEARS = "5y"
        MAX = "max"

    def __init__(self, api_key):
        self.session = requests.Session()
        self.session.headers.update({'X-API-KEY': api_key})

    def get_history(
        self,
        symbols=str,
        interval: INTERVAL = INTERVAL.ONE_DAY,
        range_: RANGE = RANGE.THREE_MONTHS,
    ):
        url = self.URLS.history.value.format(
            interval=interval.value,
            range=range_.value,
            symbols=symbols,
        )
        response = self.session.get(url)
        response.raise_for_status()
        r_json = response.json()
        data = {}
        for symbol_data in r_json.values():
            symbol_df = pd.DataFrame(
                data={
                    "timestamp": [
                        datetime.fromtimestamp(o)
                        for o in symbol_data["timestamp"]
                    ],
                    "close":
                    symbol_data["close"],
                    "symbol": [symbol_data["symbol"]] *
                    len(symbol_data["close"]),
                })
            data[symbol_data["symbol"]] = symbol_df
        return data


class TechnicalAnalysis():

    sma_field_name = "{window}_sma"

    def __init__(self, symbol: str, df: pd.DataFrame):
        self.symbol = symbol
        self.df = df

    def get_sma(self, window: int, field_name: str):
        window_field_name = self.sma_field_name.format(window=window)
        self.df[window_field_name] = self.df[field_name].rolling(
            window=window).mean()
        return window_field_name

    def sma_analysis(self, x: int, y: int, field_name: str = "close"):
        inception_points = []
        x_field_name = self.get_sma(window=x, field_name=field_name)
        y_field_name = self.get_sma(window=y, field_name=field_name)
        self.df[f"{x}_gt_{y}"] = self.df[x_field_name] > self.df[y_field_name]
        self.df[f"{x}_gt_{y}_prev"] = self.df[f"{x}_gt_{y}"].shift(1)
        self.df[f"{x}_gt_{y}_next"] = self.df[f"{x}_gt_{y}"].shift(-1)
        self.df[f"{field_name}_prev"] = self.df[field_name].shift(1)
        self.df[f"{field_name}_next"] = self.df[field_name].shift(-1)
        for index, row in self.df.iterrows():
            if (pd.notna(row[x_field_name]) and pd.notna(row[y_field_name])
                    and row[f"{x}_gt_{y}"] is True
                    and row[f"{x}_gt_{y}_prev"] is False):
                obj = {
                    "Symbol": self.symbol,
                    "Date": str(row["timestamp"].date()),
                    "Type": "BREAKOUT",
                    field_name: row[field_name],
                    f"Prev {field_name}": row[f"{field_name}_prev"],
                    f"{x} days SMA": row[x_field_name],
                    f"{y} days SMA": row[y_field_name]
                }
                inception_points.append(obj)
            elif (pd.notna(row[x_field_name]) and pd.notna(row[y_field_name])
                  and row[f"{x}_gt_{y}"] is False
                  and row[f"{x}_gt_{y}_next"] is True):
                obj = {
                    "Symbol": self.symbol,
                    "Date": str(row["timestamp"].date()),
                    "Type": "BREAKDOWN",
                    field_name: row[field_name],
                    f"Prev {field_name}": row[f"{field_name}_prev"],
                    f"{x} days SMA": row[x_field_name],
                    f"{y} days SMA": row[y_field_name]
                }
                inception_points.append(obj)
        return inception_points


API_KEY = "WaoWPn5wa82yALZGHSwOb45VkmqR4XK73cOXd8Vt"

SCRIPT_NAMES = ["HDFCLIFE.NS", "IOC.NS"]

end = date.today()
start = end - timedelta(days=10)

yf = YahooFinance(api_key=API_KEY)

if __name__ == "__main__":
    symbols_data = {}

    symbols_data = yf.get_history(
      symbols=",".join(SCRIPT_NAMES),
      range_=YahooFinance.RANGE.SIX_MONTHS,
    )
    # with open("data.dump", "wb") as file:
    # pickle.dump(symbols_data, file)
    # with open("data.dump", "rb") as file:
    # symbols_data = pickle.load(file)
    for symbol, df in symbols_data.items():
        ta = TechnicalAnalysis(symbol, df)
        inception_points = ta.sma_analysis(x=9, y=20)
        print("".center(50, "-"))
        pprint(inception_points)
