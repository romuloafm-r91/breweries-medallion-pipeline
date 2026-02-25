import requests
from typing import List, Dict


BASE_URL = "https://api.openbrewerydb.org/v1/breweries"


def fetch_breweries(per_page: int = 200) -> List[Dict]:
    """
    Fetch all breweries from Open Brewery DB API handling pagination.
    """
    all_data = []
    page = 1

    while True:
        response = requests.get(
            BASE_URL,
            params={"page": page, "per_page": per_page},
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_data.extend(data)
        page += 1

    return all_data