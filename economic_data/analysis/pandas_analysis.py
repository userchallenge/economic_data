import pandas as pd
from economic_data.load.load_data import get_indicator_data


def load_indicator_df(indicator_id: int) -> pd.DataFrame:
    raw_data = get_indicator_data(indicator_id)
    df = pd.DataFrame(
        [
            {"date": row.date, "value": row.value, "frequency": row.frequency}
            for row in raw_data
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df


def analyze_trend(df: pd.DataFrame):
    df["rolling_mean"] = df["value"].rolling(window=3).mean()
    return df
