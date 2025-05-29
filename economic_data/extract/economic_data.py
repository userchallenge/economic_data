API_KEY_FRED = "42268b215ff43235d6f0b60aaf9008a4"

import pandas as pd
import requests
import json
import logging

logger = logging.getLogger(__name__)


def label_and_append(df, indicator, source, unit, dfs_to_merge):
    """
    Labels the DataFrame with indicator, source, and unit, then appends to list if not empty.
    """
    if df is not None and not df.empty:
        df["indicator"] = indicator
        df["source"] = source
        df["unit"] = unit
        dfs_to_merge.append(df)


def fetch_eurostat_data(data_code):
    """
    Fetches data from Eurostat API using the statistics/1.0/data endpoint
    and adjusted JSON parsing logic based on user's working code.

    Args:
        data_code (str): The dataset code from Eurostat.

    Returns:
        pandas.DataFrame: DataFrame with monthly data, or None if error.
    """
    logger.info(f"Fetching Eurostat data for {data_code}")
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{data_code}?geo=EU27_2020&sinceTimePeriod=2019-01&format=JSON"  # Using statistics/1.0/data endpoint
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data_json = response.json()

        time_mapping = data_json["dimension"]["time"]["category"]["index"]
        time_list = sorted(
            time_mapping.keys(), key=lambda x: time_mapping[x]
        )  # Sort by index
        available_indexes = set(
            map(int, data_json.get("value", {}).keys())
        )  # Available indexes

        months, values = [], []

        for i, time in enumerate(time_list):
            if i in available_indexes:
                months.append(time)
                values.append(data_json["value"][str(i)])
            else:
                months.append(time)
                values.append(
                    None
                )  # Use None instead of "No data" for Pandas compatibility

        df = pd.DataFrame(
            {"date": months, "value": values}
        ).dropna()  # Create DataFrame and remove NaN rows

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])  # Format date
        logger.info(f"Fetched {len(df)} records for {data_code}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching Eurostat data for {data_code}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Eurostat for {data_code}: {e}")
        return None


def fetch_ecb_data(dataflow_ref, series_key):
    """
    Fetches data from ECB Data Portal API, handling the new JSON structure.

    Args:
        dataflow_ref (str): Dataflow reference (e.g., 'FM').
        series_key (str): Series key (e.g., 'B.U2.EUR.4F.KR.MRR_FR.LEV').

    Returns:
        pandas.DataFrame: DataFrame with monthly data, or None if error.
    """
    logger.info(f"Fetching ECB data for {dataflow_ref} - {series_key}")
    url = f"https://data-api.ecb.europa.eu/service/data/{dataflow_ref}/{series_key}?format=jsondata&startPeriod=2019-01&endPeriod=2024-12"  # Added startPeriod and endPeriod
    headers = {"Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data_json = response.json()

        # print("--- ECB data_json ---") # REMOVE DEBUG PRINT
        # print(json.dumps(data_json, indent=2)) # REMOVE DEBUG PRINT

        monthly_data = []

        time_periods_list = data_json["structure"]["dimensions"]["observation"][0][
            "values"
        ]  # List of time period objects
        series_data = next(
            iter(data_json["dataSets"][0]["series"].values())
        )  # Get the series data dictionary # CORRECTED LINE - removed data_json['data']

        observations = series_data["observations"]

        for period_index_str, value_list in observations.items():
            period_index = int(period_index_str)  # Convert index string to integer
            time_period_obj = time_periods_list[
                period_index
            ]  # Get time period object from list
            time_period = time_period_obj["id"]  # Extract time period ID
            indicator_value = (
                value_list[0] if value_list and value_list[0] is not None else None
            )  # Get first value, handle nulls

            if indicator_value is not None:  # Only append if there's a value
                monthly_data.append({"date": time_period, "value": indicator_value})

        df = pd.DataFrame(monthly_data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        logger.info(f"Fetched {len(df)} records for {dataflow_ref} - {series_key}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching ECB data for {dataflow_ref} - {series_key}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from ECB for {dataflow_ref} - {series_key}: {e}")
        return None


def fetch_fred_data(series_id, api_key, from_date="2019-01-01"):
    """
    Fetches data from FRED API.

    Args:
        series_id (str): FRED series ID.
        api_key (str): FRED API key.

    Returns:
        pandas.DataFrame: DataFrame with monthly data, or None if error.
    """
    logger.info(f"Fetching FRED data for {series_id} from {from_date}")
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&frequency=m"  # Request monthly data
    try:
        response = requests.get(url)
        response.raise_for_status()
        data_json = response.json()
        observations = data_json["observations"]

        monthly_data = []
        for obs in observations:
            if obs["value"] != ".":  # Handle missing values represented as '.'
                monthly_data.append({"date": obs["date"], "value": float(obs["value"])})

        df = pd.DataFrame(monthly_data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])  # Format date
            df = df[df["date"] >= from_date]  # Filter by from_date
        logger.info(f"Fetched {len(df)} records for {series_id}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching FRED data for {series_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from FRED for {series_id}: {e}")
        return None


def calculate_monthly_change(df, indicator_name):
    """
    Calculates the month-over-month percentage change for the given indicator data.

    Args:
        df (pd.DataFrame): DataFrame with 'date', 'value', 'indicator', 'source', and 'unit' columns.
        indicator_name (str): The name of the indicator to calculate the monthly change for.

    Returns:
        pd.DataFrame: DataFrame with new rows containing the monthly percentage change.
    """
    df = df.copy()
    if df.empty:
        return df  # Return empty DataFrame if input is empty

    df = df[df["indicator"] == indicator_name].copy()  # Filter and copy relevant data
    df = df.sort_values(by="date")  # Ensure data is sorted by date
    df["Previous_Value"] = df["value"].shift(
        1
    )  # Shift value column to get previous month's value
    df["value"] = (
        (df["value"] - df["Previous_Value"]) / df["Previous_Value"]
    ) * 100  # Calculate percentage change

    # Remove the first row as it will have NaN for 'Previous_Value' and 'value'
    df = df.iloc[1:]

    # Update the indicator name
    df["indicator"] = df["indicator"] + " (Monthly rate of change)"
    df["unit"] = "Percent"  # Set unit to "Percent"

    # Drop the 'Previous_Value' column
    df = df.drop(columns=["Previous_Value"])

    return df


# # Assuming you have your US CPI data in a DataFrame called 'us_cpi_df'
# us_cpi_df_with_change = calculate_cpi_monthly_change(
#     us_cpi_df.copy()
# )  # Use .copy() to avoid modifying original DataFrame

# print(us_cpi_df_with_change)


# --- Main script ---
# if __name__ == "__main__":
def fetch_economic_data():
    api_key_fred = API_KEY_FRED

    if api_key_fred == "YOUR_FRED_API_KEY":
        print(
            "Please replace 'YOUR_FRED_API_KEY' with your actual FRED API key to fetch FRED data."
        )

    # --- Fetch Eurostat Data ---
    hicp_euro_df = fetch_eurostat_data("prc_hicp_mmor")
    unemployment_euro_df = fetch_eurostat_data("ei_lmhr_m")

    # --- Fetch ECB Data ---
    ecb_interest_rate_df = fetch_ecb_data("FM", "B.U2.EUR.4F.KR.MRR_FR.LEV")

    # --- Fetch FRED Data ---
    us_unemployment_df = fetch_fred_data("UNRATE", api_key_fred)
    us_cpi_df = fetch_fred_data("CPIAUCSL", api_key_fred)
    us_fed_funds_rate_df = fetch_fred_data("DFF", api_key_fred)

    # --- Prepare to merge and label data ---
    dfs_to_merge = []

    label_and_append(
        hicp_euro_df,
        "Eurozone HICP (Monthly Rate of Change)",
        "Eurostat",
        "Percent",
        dfs_to_merge,
    )
    label_and_append(
        unemployment_euro_df,
        "Eurozone Unemployment Rate",
        "Eurostat",
        "Percent",
        dfs_to_merge,
    )
    label_and_append(
        ecb_interest_rate_df,
        "Eurozone Interest Rate (Main Refinancing Operations)",
        "ECB",
        "Percent per annum",
        dfs_to_merge,
    )
    label_and_append(
        us_unemployment_df,
        "US Unemployment Rate",
        "FRED",
        "Percent",
        dfs_to_merge,
    )
    label_and_append(
        us_cpi_df,
        "US CPI",
        "FRED",
        "Index",
        dfs_to_merge,
    )
    label_and_append(
        us_fed_funds_rate_df,
        "US Federal Funds Rate",
        "FRED",
        "Percent",
        dfs_to_merge,
    )

    # --- Concatenate all DataFrames ---
    if dfs_to_merge:
        final_df = pd.concat(dfs_to_merge, ignore_index=True)
        return final_df
    else:
        print("No data fetched successfully. Please check for API errors and API keys.")
        return None


if __name__ == "__main__":

    fetch_economic_data()
