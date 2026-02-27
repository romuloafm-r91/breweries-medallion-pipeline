import pandas as pd
from src.gold import aggregate_breweries


def test_gold_aggregation_counts_correctly():
    df = pd.DataFrame({
        "country": ["us", "us"],
        "state": ["ny", "ny"],
        "brewery_type": ["micro", "micro"]
    })

    result = aggregate_breweries(df)

    assert result["total_breweries"].iloc[0] == 2
    assert result.shape[0] == 1