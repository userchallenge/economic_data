import pandas as pd
import logging

logger = logging.getLogger(__name__)


def eurostat_json_to_df(data_json, data_code):
    """
    Transforms Eurostat JSON to DataFrame.
    """
    try:
        time_mapping = data_json["dimension"]["time"]["category"]["index"]
        time_list = sorted(time_mapping.keys(), key=lambda x: time_mapping[x])
        available_indexes = set(map(int, data_json.get("value", {}).keys()))
        months, values = [], []
        for i, time in enumerate(time_list):
            if i in available_indexes:
                months.append(time)
                values.append(data_json["value"][str(i)])
            else:
                months.append(time)
                values.append(None)
        df = pd.DataFrame({"date": months, "value": values})
        df = df.dropna()
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        logger.info(f"Transformed Eurostat data for {data_code}: {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error processing Eurostat data for {data_code}: {e}")
        return None


def ecb_json_to_df(data_json, dataflow_ref, series_key):
    """
    Transforms ECB JSON to DataFrame.
    """
    try:
        monthly_data = []
        time_periods_list = data_json["structure"]["dimensions"]["observation"][0][
            "values"
        ]
        series_data = next(iter(data_json["dataSets"][0]["series"].values()))
        observations = series_data["observations"]
        for period_index_str, value_list in observations.items():
            period_index = int(period_index_str)
            time_period_obj = time_periods_list[period_index]
            time_period = time_period_obj["id"]
            indicator_value = (
                value_list[0] if value_list and value_list[0] is not None else None
            )
            if indicator_value is not None:
                monthly_data.append({"date": time_period, "value": indicator_value})
        df = pd.DataFrame(monthly_data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        logger.info(
            f"Transformed ECB data for {dataflow_ref} - {series_key}: {len(df)} records"
        )
        return df
    except Exception as e:
        logger.error(
            f"Error processing ECB data for {dataflow_ref} - {series_key}: {e}"
        )
        return None


def fred_json_to_df(data_json, from_date):
    """
    Transforms FRED JSON to DataFrame.

    Parameters:
    - data_json: JSON data from FRED API.
    - from_date: datetime-like or string (e.g., "2015-01-01"). If provided,
                 it sets the lower bound of the monthly time series.
    Returns:
    - DataFrame with 'date' and 'value' columns, filtered by `from_date`.
    If `from_date` is not provided, it returns all records.
    """
    try:
        observations = data_json["observations"]
        monthly_data = []
        for obs in observations:
            if obs["value"] != ".":
                monthly_data.append({"date": obs["date"], "value": float(obs["value"])})
        df = pd.DataFrame(monthly_data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= from_date]
        logger.info(f"Transformed FRED data: {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error processing FRED data: {e}")
        return None


def label_and_append(df, indicator, source, unit, dfs_to_merge):
    """
    Labels the DataFrame with indicator, source, and unit, then appends to list if not empty.
    """
    if df is not None and not df.empty:
        df["indicator"] = indicator
        df["source"] = source
        df["unit"] = unit
        dfs_to_merge.append(df)


def calculate_monthly_change(df, indicator_name):
    """
    Calculates the month-over-month percentage change for the given indicator data.
    """
    df = df.copy()
    if df.empty:
        return df
    df = df[df["indicator"] == indicator_name].copy()
    df = df.sort_values(by="date")
    df["Previous_Value"] = df["value"].shift(1)
    df["value"] = ((df["value"] - df["Previous_Value"]) / df["Previous_Value"]) * 100
    df = df.iloc[1:]
    df["indicator"] = df["indicator"] + " (Monthly rate of change)"
    df["unit"] = "Percent"
    df = df.drop(columns=["Previous_Value"])
    return df


def set_monthly_ecb_interest_rate(df, start_date=None):
    """
    Converts ECB interest rate data to monthly frequency starting from `start_date`,
    filling in interest rates from the latest known rate prior to each month.

    Parameters:
    - df: DataFrame containing ECB interest rates with 'date' and 'value' columns.
    - start_date: optional datetime-like or string (e.g., "2015-01-01"). If provided,
                  it sets the lower bound of the monthly time series and fills
                  earlier months with the first known interest rate.

    Returns:
    - monthly: DataFrame with one row per month starting from `start_date`,
               and interest rate values forward- and back-filled as needed.
    """
    mask_ECB = df["indicator"] == "Eurozone Interest Rate (Main Refinancing Operations)"

    df = df[mask_ECB].copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Determine full date range
    start = (
        pd.to_datetime(start_date).replace(day=1)
        if start_date
        else df["date"].min().replace(day=1)
    )
    end = pd.Timestamp.today().replace(day=1)

    # Create regular first-of-month date range
    monthly = pd.DataFrame({"month_start": pd.date_range(start, end, freq="MS")})

    # Reset index to use merge_asof
    df = df.reset_index(drop=True)

    # Merge: find latest past interest rate for each month
    monthly = pd.merge_asof(
        monthly, df, left_on="month_start", right_on="date", direction="backward"
    )

    # Backfill months before first change using the earliest known rate
    # monthly["value"] = monthly["value"].fillna(method="bfill")
    monthly = monthly.fillna(method="bfill")

    # Final cleanup
    monthly["date"] = monthly["month_start"]
    monthly = monthly.drop(columns=["month_start", "date_y"], errors="ignore")

    return monthly


def rename_economic_indicators(df):
    """
    Rename economic indicators.

    "Eurozone HICP (Monthly Rate of Change)": "inflation_monthly_euro",
    "US CPI (Monthly Rate of Change)": "inflation_monthly_us",
    "Eurozone Monthly Interest Rate (Main Refinancing Operations)": "interest_rate_monthly_euro",
    "US Federal Funds Rate": "interest_rate_monthly_us",
    "Eurozone Unemployment Rate": "unemployment_rate_monthly_euro",
    "US Unemployment Rate": "unemployment_monthly_rate_us",
    "US CPI": "inflation_index_monthly_us",
    "Eurozone Interest Rate (Main Refinancing Operations)": "interest_rate_change_day_euro",
    """
    renaming_map = {
        "Eurozone HICP (Monthly Rate of Change)": "inflation_monthly_euro",
        "US CPI (Monthly Rate of Change)": "inflation_monthly_us",
        "Eurozone Monthly Interest Rate (Main Refinancing Operations)": "interest_rate_monthly_euro",
        "US Federal Funds Rate": "interest_rate_monthly_us",
        "Eurozone Unemployment Rate": "unemployment_rate_monthly_euro",
        "US Unemployment Rate": "unemployment_monthly_rate_us",
        "US CPI": "inflation_index_monthly_us",
        "Eurozone Interest Rate (Main Refinancing Operations)": "interest_rate_change_day_euro",
    }
    df["indicator"] = df["indicator"].replace(renaming_map)
    return df
