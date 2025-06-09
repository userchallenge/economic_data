from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def convert_google_finance_index_to_dict(index_data, name, description, source):
    """
    Convert indecies data from google finance via historisk_data worksheet to
    dictionary that fits with the models/database schema.

    Args:

        index_data (list of lists): The index data from the Google Sheet in raw format.
        name (str): The name of the index.
        description (str): A description of the index.
        source (str): The source of the index data.
    Returns:
        dict: A dictionary containing the index data with keys 'ticker_id', 'name', 'description', and 'source'.


    """
    index_dict = {
        "ticker_id": index_data[1][0],
        "name": name,
        "description": description,
        "source": source,
    }
    logger.info(
        f"Converting index data for {index_dict['name']} ({index_dict['ticker_id']})"
    )
    return index_dict


def convert_google_finance_data_to_dict(index_data):
    """
    Convert stock market data from Google Finance into a list of dictionaries.
    Each dictionary contains the date, open value, high value, low value, close value, and volume.
    Args:
        index_data (list of lists): The stock market data from Google Finance in raw format.
    Returns:
        list: A list of dictionaries, each representing a stock market data entry with keys 'date', 'open_value',
        'high_value', 'low_value', 'close_value', and 'volume'.
    The 'date' is converted to a datetime.date object, and numeric values are converted to floats.
    The 'volume' is converted to an integer if possible, otherwise set to 0.

    """
    raw_data = index_data[1:]
    data = []
    for row in raw_data:
        data.append(
            {
                "date": row[1],
                "open_value": row[2],
                "high_value": row[3],
                "low_value": row[4],
                "close_value": row[5],
                "volume": row[6],
            }
        )
    # Convert date strings to datetime.date objects
    for row in data:
        # If your DB expects datetime, use datetime.strptime(...), else use .date()
        row["date"] = datetime.strptime(row["date"], "%Y-%m-%d %H.%M.%S").date()

    def convert_to_float(val):
        if isinstance(val, str):
            return float(val.replace(",", "."))
        return val

    for entry in data:
        for key in ["open_value", "high_value", "low_value", "close_value"]:
            entry[key] = convert_to_float(entry[key])
        # Optionally convert volume to int if needed
        if "volume" in entry:
            try:
                entry["volume"] = int(entry["volume"])
            except ValueError:
                entry["volume"] = 0
    logger.info(
        f"Converted {len(data)} rows of stock market data to dictionary format."
    )

    return data
