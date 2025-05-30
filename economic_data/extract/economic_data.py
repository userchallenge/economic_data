# API_KEY_FRED = "42268b215ff43235d6f0b60aaf9008a4"

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


def fetch_eurostat_json(data_code):
    """
    Fetches raw JSON data from Eurostat API.

    Eurostat:
    prc_hicp_midx: Harmonised Index of Consumer Prices (HICP) for Euro area (monthly).
    une_rt_m: Unemployment rate for Euro area (monthly).

    """
    logger.info(f"Fetching Eurostat data for {data_code}")
    # TODO: Centralize date range handling
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{data_code}?geo=EU27_2020&sinceTimePeriod=2019-01&format=JSON"
    return fetch_json(url)


def fetch_ecb_json(dataflow_ref, series_key):
    """
    Fetches raw JSON data from ECB Data Portal API.

    FM, B.U2.EUR.4F.KR.MRR_FR.LEV: Main Refinancing Operations interest rate for Euro area (monthly).

    """
    # TODO: Centralize date range handling
    url = f"https://data-api.ecb.europa.eu/service/data/{dataflow_ref}/{series_key}?format=jsondata&startPeriod=2019-01&endPeriod=2024-12"
    headers = {"Accept": "application/json"}
    return fetch_json(url, headers=headers)


def fetch_fred_json(series_id, api_key):
    """
    Fetches raw JSON data from FRED API.

    UNRATE: US Unemployment Rate (monthly).
    CPIAUCSL: US Consumer Price Index (CPI) (monthly).
    DFF: US Federal Funds Rate (monthly).
    """
    logger.info(f"Fetching FRED data for series ID {series_id}")
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&frequency=m"
    return fetch_json(url)
