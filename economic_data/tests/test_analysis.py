import pandas as pd
from economic_data.analyse.pandas_analysis import (
    load_indicator_df,
    calculate_rolling_average,
)
from economic_data.load.save_data import save_economic_indicator, save_indicator_data


def test_rolling_average():
    indicator_id = save_economic_indicator(
        {"name": "Retail Sales", "unit": "SEK", "description": "Retail sales test"}
    )

    save_indicator_data(
        indicator_id,
        [
            {"date": "2023-01-01", "value": 100, "frequency": "monthly"},
            {"date": "2023-02-01", "value": 200, "frequency": "monthly"},
            {"date": "2023-03-01", "value": 300, "frequency": "monthly"},
        ],
    )

    df = load_indicator_df(indicator_id)
    df = calculate_rolling_average(df)
    assert "rolling_avg" in df.columns
    # First two rolling average values are NaN for window=3
    assert pd.isna(df.iloc[0]["rolling_avg"])
    assert pd.isna(df.iloc[1]["rolling_avg"])
    assert df.iloc[2]["rolling_avg"] == 200.0
