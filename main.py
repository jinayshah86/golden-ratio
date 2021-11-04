import os
import pickle
from pprint import pprint

from yahoo_finance import YahooFinance
from technical_analysis import TechnicalAnalysis

API_KEY = os.environ['YAHOO_FINANCE_API_KEY']
SCRIPT_NAMES = ["HDFCLIFE.NS", "IOC.NS"]


def get_live_data():
    yf = YahooFinance(api_key=API_KEY)
    return yf.get_history(
        symbols=",".join(SCRIPT_NAMES),
        range_=YahooFinance.RANGE.SIX_MONTHS,
    )


def get_test_data():
    with open("data.dump", "rb") as file:
        return pickle.load(file)


if __name__ == "__main__":
    symbols_data = get_live_data()
    for symbol, df in symbols_data.items():
        ta = TechnicalAnalysis(symbol, df)
        inception_points = ta.sma_analysis(x=9, y=20)
        print("".center(50, "-"))
        pprint(inception_points)
