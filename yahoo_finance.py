from enum import Enum
from datetime import datetime

import pandas as pd
import requests


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
