import logging

from logger_config import setup_logging

setup_logging(level=logging.INFO)


import configparser

import pandas as pd

from economic_data.extract.economic_data import (
    fetch_ecb_json,
    fetch_eurostat_json,
    fetch_fred_json,
)
from economic_data.transform.transform_economic_data import (
    calculate_monthly_change,
    ecb_json_to_df,
    eurostat_json_to_df,
    fred_json_to_df,
    label_and_append,
    set_monthly_ecb_interest_rate,
    rename_economic_indicators,
)

logger = logging.getLogger(__name__)

# config
CONFIG = "config/config.ini"
config = configparser.ConfigParser()
config.read(CONFIG)
API_KEY_FRED = config["API_KEYS"]["FRED"]


def main():
    logger.info("Starting economic data extraction and transformation...")

    # Extract
    eurostat_hicp_json = fetch_eurostat_json("prc_hicp_mmor")
    eurostat_unemployment_json = fetch_eurostat_json("ei_lmhr_m")
    ecb_interest_json = fetch_ecb_json("FM", "B.U2.EUR.4F.KR.MRR_FR.LEV")
    fred_unemployment_json = fetch_fred_json("UNRATE", API_KEY_FRED)
    fred_cpi_json = fetch_fred_json("CPIAUCSL", API_KEY_FRED)
    fred_fedfunds_json = fetch_fred_json("DFF", API_KEY_FRED)

    # Transform
    hicp_euro_df = eurostat_json_to_df(eurostat_hicp_json, "prc_hicp_mmor")
    unemployment_euro_df = eurostat_json_to_df(eurostat_unemployment_json, "ei_lmhr_m")
    ecb_interest_rate_df = ecb_json_to_df(
        ecb_interest_json, "FM", "B.U2.EUR.4F.KR.MRR_FR.LEV"
    )

    us_unemployment_df = fred_json_to_df(fred_unemployment_json)
    us_cpi_df = fred_json_to_df(fred_cpi_json)
    us_fed_funds_rate_df = fred_json_to_df(fred_fedfunds_json)

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
    # add ECB interest rate with monthly frequency
    ecb_monthly_interest_rate_df = set_monthly_ecb_interest_rate(ecb_interest_rate_df)
    label_and_append(
        ecb_monthly_interest_rate_df,
        "Eurozone Monthly Interest Rate (Main Refinancing Operations)",
        "ECB",
        "Percent per annum",
        dfs_to_merge,
    )
    label_and_append(
        us_unemployment_df, "US Unemployment Rate", "FRED", "Percent", dfs_to_merge
    )
    label_and_append(us_cpi_df, "US CPI", "FRED", "Index", dfs_to_merge)
    label_and_append(
        us_fed_funds_rate_df, "US Federal Funds Rate", "FRED", "Percent", dfs_to_merge
    )

    # Calculate monthly change for US CPI
    us_cpi_monthly_change_df = calculate_monthly_change(us_cpi_df, "US CPI")
    label_and_append(
        us_cpi_monthly_change_df,
        "US CPI (Monthly Rate of Change)",
        "FRED",
        "Percent",
        dfs_to_merge,
    )

    if dfs_to_merge:
        final_df = pd.concat(dfs_to_merge, ignore_index=True)
        # rename indicators for clarity
        final_df = rename_economic_indicators(final_df)
        logger.info(
            f"Data extraction and transformation completed successfully with final df shape {final_df.shape}."
        )
        logger.info(final_df.groupby("indicator").size())
        final_df.to_excel(
            "tmp_output/economic_data_summary.xlsx",
            index=False,
            engine="openpyxl",
        )
    else:
        logger.info("No data to show.")
    return final_df


if __name__ == "__main__":
    main()
