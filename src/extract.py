import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.models import Variable

BASE_URL = Variable.get("API_BASE_URL")
TOTAL_RETRIES = int(Variable.get("API_TOTAL_RETRIES", default_var=3))
BACKOFF_FACTOR = float(Variable.get("API_BACKOFF_FACTOR", default_var=0.5))

def create_session_with_retry(
):
    """
    Create a requests Session configured with retry strategy.

    Retries are applied only for idempotent GET requests and
    transient HTTP errors (429, 5xx).
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=TOTAL_RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=BACKOFF_FACTOR,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_breweries(per_page: int = 200):
    """
    Fetch all breweries from the API using pagination.

    Each page request is protected with retry strategy and timeout control.
    Pagination stops when the API returns an empty list.
    """
    session = create_session_with_retry()
    page = 1
    all_data = []

    while True:
        params = {
            "page": page,
            "per_page": per_page,
        }

        response = session.get(
            BASE_URL,
            params=params,
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_data.extend(data)
        page += 1

    return all_data