import pandas as pd
import logging
import numpy as np
import re

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


# SKA ERSÄTTAS MED NEDAN


def _convert_frequency_code_to_enum(frequency_str):
    # convert frequency string to Frequency enum
    from economic_data.db.schema import Frequency

    if frequency_str == "D":
        return Frequency.daily
    elif frequency_str == "M":
        return Frequency.monthly
    elif frequency_str == "Q":
        return Frequency.quarterly
    elif frequency_str == "A":
        return Frequency.yearly
    else:
        raise ValueError(f"Unknown frequency: {frequency_str}")


def convert_eurostat_infl_ind_to_dict(data_json, name, description):
    """
    Converts Eurostat inflation index data to a dictionary format.
    Parameters:
    - data_json: JSON data from Eurostat API.
    - name: Name of the indicator.
    - description: Description of the indicator.
    Returns:
    - Dictionary with keys 'indicator_id', 'name', 'description', 'unit', 'frequency', and 'source'.
    """

    index_dict = {
        "indicator_id": data_json["label"],
        "name": name,
        "description": description,
        "unit": "percent",
        "frequency": _convert_frequency_code_to_enum(
            list(data_json["dimension"]["freq"]["category"]["label"].keys())[0]
        ),
        "source": data_json["source"],
    }
    logger.info(f"Creating index data for {index_dict['name']}")
    return index_dict


# SLUT --------------------


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
    # monthly = monthly.fillna(method="bfill")
    monthly = monthly.bfill()

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


def threshold_csv_to_df(file):
    """
    Reads a CSV file containing economic thresholds and normalizes the ranges.
    The CSV should have columns like:
    - indicator: Name of the economic indicator
    - good_range: Range for "good" condition (e.g., "0.0% – 0.2% or 0.4% – 0.8%")
    - medium_range: Range for "medium" condition
    - bad_range: Range for "bad" condition
    Returns a DataFrame with normalized ranges for each condition.
    """

    thresholds_df = pd.read_csv(file)

    def parse_range_expression(indicator, label, expr):
        """
        Parses a string like "0.0% – 0.2% or 0.4% – 0.8%" into multiple numeric range rows.

        Returns a list of dicts with:
        - min_val / max_val (float or +/-inf)
        - inclusive_min / inclusive_max (bools)
        - indicator and label for later joins
        """
        parts = re.split(r"\s+or\s+", expr)
        parsed = []

        for part in parts:
            part = part.strip().replace("%", "").replace("–", "-").replace("−", "-")
            if part.startswith("<"):
                val = float(part[1:].strip())
                parsed.append(
                    {
                        "indicator": indicator,
                        "label": label,
                        "min_val": -np.inf,
                        "max_val": val,
                        "inclusive_min": False,
                        "inclusive_max": True,
                    }
                )
            elif part.startswith(">"):
                val = float(part[1:].strip())
                parsed.append(
                    {
                        "indicator": indicator,
                        "label": label,
                        "min_val": val,
                        "max_val": np.inf,
                        "inclusive_min": False,
                        "inclusive_max": False,
                    }
                )
            elif "-" in part:
                min_val, max_val = map(lambda x: float(x.strip()), part.split("-"))
                parsed.append(
                    {
                        "indicator": indicator,
                        "label": label,
                        "min_val": min_val,
                        "max_val": max_val,
                        "inclusive_min": True,
                        "inclusive_max": True,
                    }
                )

        return parsed

    # --- Step 3: Normalize the whole table ---
    normalized_rows = []
    for _, row in thresholds_df.iterrows():
        for label in ["good", "medium", "bad"]:
            expr = row[f"{label}_range"]
            normalized_rows.extend(
                parse_range_expression(row["indicator"], label, expr)
            )

    thresholds_normalized_df = pd.DataFrame(normalized_rows)

    thresholds_normalized_df = thresholds_normalized_df.drop_duplicates().reset_index(
        drop=True
    )
    label_score_map = {"good": 2, "medium": 1, "bad": 0}
    thresholds_normalized_df["score"] = thresholds_normalized_df["label"].map(
        label_score_map
    )

    return thresholds_normalized_df


def load_thresholds(df, thresholds_df):

    # --- Assign score to each row in financial data ---
    def assign_score(value, indicator, thresholds_df):
        """
        Finds the matching score for a given value and indicator.

        Args:
            value (float): The numeric value to evaluate
            indicator (str): Name of the indicator
            thresholds_df (pd.DataFrame): Normalized threshold definitions

        Returns:
            int or None: Score (0 = bad, 1 = medium, 2 = good), or None if no match
        """
        relevant = thresholds_df[thresholds_df["indicator"] == indicator]

        for _, row in relevant.iterrows():
            lower_ok = (
                value > row["min_val"]
                if not row["inclusive_min"]
                else value >= row["min_val"]
            )
            upper_ok = (
                value < row["max_val"]
                if not row["inclusive_max"]
                else value <= row["max_val"]
            )
            if lower_ok and upper_ok:
                return row["score"]
        return (
            None  # fallback if no match (shouldn't happen if thresholds are exhaustive)
        )

    # --- Run logic using your in-memory dataframes ---
    # thresholds_df and financial_df should already exist
    # thresholds_normalized_df = normalize_threshold_table(thresholds_df)

    # Apply scores in place
    df["score"] = df.apply(
        lambda row: assign_score(row["value"], row["indicator"], thresholds_df),
        axis=1,
    )

    return df
