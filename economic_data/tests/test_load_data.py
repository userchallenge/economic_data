from economic_data.load.load_data import (
    get_all_indicators,
    get_indicator_data,
    get_all_stock_indices,
    get_stock_data,
    get_thresholds_for_indicator,
)
from economic_data.load.save_data import (
    save_indicator,
    save_indicator_data,
    save_stock_index,
    save_stock_data,
    save_threshold,
)


def test_get_all_indicators():
    save_indicator({"name": "Inflation", "unit": "%", "description": "Inflation test"})
    results = get_all_indicators()
    assert any(i.name == "Inflation" for i in results)


def test_get_indicator_data():
    indicator_id = save_indicator(
        {"name": "Unemployment", "unit": "%", "description": "Unemployment test"}
    )

    save_indicator_data(
        indicator_id, [{"date": "2023-03-01", "value": 7.5, "frequency": "monthly"}]
    )

    results = get_indicator_data(indicator_id)
    assert len(results) == 1
    assert results[0].value == 7.5


def test_get_all_stock_indices():
    save_stock_index({"name": "Dow Jones", "description": "Dow Jones test"})
    results = get_all_stock_indices()
    assert any(i.name == "Dow Jones" for i in results)


def test_get_stock_data():
    index_id = save_stock_index(
        {"name": "FTSE 100", "description": "UK stock market index"}
    )

    save_stock_data(
        index_id, [{"date": "2023-03-01", "value": 7000.0, "frequency": "daily"}]
    )

    results = get_stock_data(index_id)
    assert len(results) == 1
    assert results[0].value == 7000.0


def test_get_thresholds_for_indicator():
    indicator_id = save_indicator(
        {"name": "GDP Growth", "unit": "%", "description": "GDP growth test"}
    )
    save_threshold(
        {
            "indicator_id": indicator_id,
            "good_min": 2.0,
            "good_max": 5.0,
            "normal_min": 0.5,
            "normal_max": 6.0,
            "bad_min": None,
            "bad_max": None,
            "description": "GDP thresholds",
        }
    )
    thresholds = get_thresholds_for_indicator(indicator_id)
    assert len(thresholds) > 0
    assert thresholds[0].good_min == 2.0
