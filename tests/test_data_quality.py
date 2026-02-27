import pandas as pd
import pytest
from src.data_quality import validate_silver_dataframe


thresholds = {
    "max_lat_null_pct": 0.5,
    "max_unknown_country_pct": 0.1,
}

def test_empty_dataframe_fails():
    df = pd.DataFrame()
    with pytest.raises(ValueError):
        validate_silver_dataframe(df, thresholds)


def test_duplicate_ids_fail():
    df = pd.DataFrame({
        "id": [1, 1],
        "latitude": [10.0, 20.0],
        "country": ["us", "us"]
    })
    with pytest.raises(ValueError):
        validate_silver_dataframe(df, thresholds)


def test_valid_dataframe_passes():
    df = pd.DataFrame({
        "id": [1, 2],
        "latitude": [10.0, 20.0],
        "country": ["us", "us"]
    })

    validate_silver_dataframe(df, thresholds)