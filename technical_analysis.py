import pandas as pd

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
