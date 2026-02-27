import pandas as pd
from src.silver import clean_breweries_dataframe


def test_remove_duplicates():
    df = pd.DataFrame({
        "id": [1, 1],
        "name": ["A", "A"],
        "brewery_type": ["Micro", "Micro"],
        "city": ["NY", "NY"],
        "state": ["New York", "New York"],
        "country": ["United States", "United States"],
        "latitude": ["40.7", "40.7"],
        "longitude": ["-73.9", "-73.9"],
    })

    result = clean_breweries_dataframe(df)

    assert len(result) == 1


def test_string_standardization():
    df = pd.DataFrame({
        "id": [1],
        "name": [" Test Brewery "],
        "brewery_type": [" Micro "],
        "city": [" New York "],
        "state": [" New York "],
        "country": [" United States "],
        "latitude": ["40.7"],
        "longitude": ["-73.9"],
    })

    result = clean_breweries_dataframe(df)

    assert result["brewery_type"].iloc[0] == "micro"
    assert result["city"].iloc[0] == "new york"
    assert result["state"].iloc[0] == "new_york"
    assert result["country"].iloc[0] == "united_states"