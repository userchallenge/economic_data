import logging


from logger_config import setup_logging

setup_logging(level=logging.INFO)


import configparser

import pandas as pd


from economic_data.extract.economic_data import (
    fetch_ecb_json,
    fetch_eurostat_json,
    fetch_fred_json,
    get_historical_stock_data,
)
from economic_data.transform.transform_economic_data import (
    calculate_monthly_change,
    ecb_json_to_df,
    eurostat_json_to_df,
    fred_json_to_df,
    label_and_append,
    set_monthly_ecb_interest_rate,
    rename_economic_indicators,
    threshold_csv_to_df,
    load_thresholds,
)

from economic_data.transform.transform_stockmarket_data import (
    convert_google_finance_index_to_dict,
    convert_google_finance_data_to_dict,
)
from economic_data.transform.transform_economic_data import (
    convert_eurostat_infl_ind_to_dict,
    convert_eurostat_infl_data_to_dict,
    # convert_eurostat_unemployment_to_dict,
    # convert_eurostat_gdp_to_dict,
)

from economic_data.load.save_data import (
    save_stock_index,
    save_stock_data,
    save_indicator,
    save_indicator_data,
)

logger = logging.getLogger(__name__)

# config
CONFIG = "config/config.ini"
config = configparser.ConfigParser()
config.read(CONFIG)

API_KEY_FRED = config["API_KEYS"]["FRED"]
FROM_DATE = config["DATE_RANGE"]["FROM_DATE"]
TO_DATE = config["DATE_RANGE"]["TO_DATE"]
THRESHOLD_FILE = config["FILES"]["ECONOMIC_THRESHOLDS"]
SERVICE_ACCOUNT_FILE = config["GOOGLE_HISTORICAL_DATA"]["API_KEY_FILE"]
SPREADSHEET_ID = config["GOOGLE_HISTORICAL_DATA"]["ID"]


def main():
    logger.info("Starting economic data extraction and transformation...")

    # Extract - economic indicators
    inflation_euro_json = fetch_eurostat_json("prc_hicp_mmor", FROM_DATE)
    eurostat_unemployment_json = fetch_eurostat_json("ei_lmhr_m", FROM_DATE)
    ecb_interest_json = fetch_ecb_json(
        "FM", "B.U2.EUR.4F.KR.MRR_FR.LEV", FROM_DATE, TO_DATE
    )
    fred_unemployment_json = fetch_fred_json("UNRATE", API_KEY_FRED, FROM_DATE, TO_DATE)
    fred_cpi_json = fetch_fred_json("CPIAUCSL", API_KEY_FRED, FROM_DATE, TO_DATE)
    fred_fedfunds_json = fetch_fred_json("DFF", API_KEY_FRED, FROM_DATE, TO_DATE)

    # Extract - Stocks and Indices
    omx_smi_dict = get_historical_stock_data(
        "INDEXNASDAQ:OMXSPI",
        SERVICE_ACCOUNT_FILE,
        SPREADSHEET_ID,
        FROM_DATE,
    )

    # Transform - Economic Indicators
    # TODO: Döp om alla namn på formatet <KPI><Region><typ av data>
    # T ex inflation_euro_indicator_id
    inflation_euro_df = eurostat_json_to_df(inflation_euro_json, "prc_hicp_mmor")
    inflation_euro_indicator = convert_eurostat_infl_ind_to_dict(
        inflation_euro_json,
        "inflation_monthly_euro",
        "Monthly inflation rate in EURO area",
    )
    inflation_euro_data = convert_eurostat_infl_data_to_dict(
        inflation_euro_json, "prc_hicp_mmor"
    )

    # inflation_euro_df = rename_economic_indicators(
    #     inflation_euro_df
    # )  # TODO: Denna ska nog tas bort
    # NEXT: Fixa resten av indicators
    unemployment_euro_df = eurostat_json_to_df(eurostat_unemployment_json, "ei_lmhr_m")
    ecb_interest_rate_df = ecb_json_to_df(
        ecb_interest_json, "FM", "B.U2.EUR.4F.KR.MRR_FR.LEV"
    )
    us_unemployment_df = fred_json_to_df(fred_unemployment_json, FROM_DATE)
    us_cpi_df = fred_json_to_df(fred_cpi_json, FROM_DATE)
    us_fed_funds_rate_df = fred_json_to_df(fred_fedfunds_json, FROM_DATE)
    thresholds_df = threshold_csv_to_df(THRESHOLD_FILE)

    # Transform - Stocks and Indices
    index_omx = convert_google_finance_index_to_dict(
        omx_smi_dict, "omx_smi", "stokcholms index", "google spreadsheet"
    )

    data_omx = convert_google_finance_data_to_dict(omx_smi_dict)

    # Load - Save stock data
    index_omx_id = save_stock_index(
        index_omx,
    )
    save_stock_data(index_omx_id, data_omx)

    # Load - Save economic indicator data
    inflation_euro_indicator_id = save_indicator(inflation_euro_indicator)
    save_indicator_data(inflation_euro_indicator_id, inflation_euro_data)

    # -------------------

    dfs_to_merge = []
    label_and_append(
        inflation_euro_df,
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
    ecb_monthly_interest_rate_df = set_monthly_ecb_interest_rate(
        ecb_interest_rate_df, FROM_DATE
    )
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

    else:
        logger.info("No data to show.")

    # Load thresholds
    final_df = load_thresholds(final_df, thresholds_df)
    # final_df.to_excel(
    #     "tmp_output/economic_data_summary.xlsx",
    #     index=False,
    #     engine="openpyxl",
    # )
    return final_df


if __name__ == "__main__":
    main()
