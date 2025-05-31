import pandas as pd
import requests
import json
import logging

logger = logging.getLogger(__name__)


def fetch_json(url, headers=None):
    """
    Generic helper to fetch JSON data from a URL with error handling.
    Returns the parsed JSON or None if there was an error.
    """
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {url}: {e}")
        return None


def _to_year_month(date_str):
    """
    Converts a date string to a 'YYYY-MM' format. If the input is empty,
    it returns the current date in 'YYYY-MM' format.

    Parameters:
    ----------
    date_str : str
        The date string in 'YYYY-MM-DD' format.

    Returns:
    -------
    str
        The date in 'YYYY-MM' format.
    """
    if not date_str:
        # return todays date
        today = pd.Timestamp.today()
        return pd.to_datetime(today).strftime("%Y-%m")
    else:
        return pd.to_datetime(date_str).strftime("%Y-%m")


def fetch_eurostat_json(data_code: str, from_date: str):
    """
    Fetches raw JSON data from the Eurostat API for a specific indicator and time range.

    Parameters:
    ----------
    data_code : str
        The Eurostat dataset code to fetch. Examples include:
        - "prc_hicp_midx": Harmonised Index of Consumer Prices (HICP) for the Euro area (monthly)
        - "une_rt_m": Unemployment rate for the Euro area (monthly)
    from_date : str
        The start date for the data range in 'YYYY-MM-DD' format.
        Note: Only the year and month are used; the day is ignored.

    Returns:
    -------
    dict or None
        The raw JSON response from the Eurostat API as a Python dictionary,
        or None if the request fails or the response cannot be decoded.
    """

    logger.info(
        f"Fetching Eurostat data for {data_code} and from {_to_year_month(from_date)}"
    )

    url = (
        f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{data_code}"
        f"?geo=EU27_2020"
        f"&sinceTimePeriod={_to_year_month(from_date)}"
        f"&format=JSON"
    )
    return fetch_json(url)


def fetch_ecb_json(dataflow_ref, series_key, from_date, to_date):
    """
    Fetches raw JSON data from ECB Data Portal API.

    FM, B.U2.EUR.4F.KR.MRR_FR.LEV: Main Refinancing Operations interest rate for Euro area (monthly).

    """

    logger.info(
        f"Fetching ECB data for dataflow {dataflow_ref}, series {series_key}, "
        f"from {_to_year_month(from_date)} to {_to_year_month(to_date)}"
    )
    url = (
        f"https://data-api.ecb.europa.eu/service/data/{dataflow_ref}/{series_key}"
        f"?format=jsondata&startPeriod={_to_year_month(from_date)}"
        f"&endPeriod={_to_year_month(to_date)}"
    )

    headers = {"Accept": "application/json"}
    return fetch_json(url, headers=headers)


def fetch_fred_json(series_id, api_key, from_date, to_date):
    """
    Fetches raw JSON data from FRED API.

    UNRATE: US Unemployment Rate (monthly).
    CPIAUCSL: US Consumer Price Index (CPI) (monthly).
    DFF: US Federal Funds Rate (monthly).
    """
    logger.info(
        f"Fetching FRED data for series ID {series_id}, from {from_date} to {to_date}"
    )
    url = (
        f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
        f"&api_key={api_key}"
        f"&file_type=json&frequency=m"
    )
    logger.debug(f"FRED URL: {url}")
    return fetch_json(url)
