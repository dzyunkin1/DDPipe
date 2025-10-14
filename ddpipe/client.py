import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class DDClient:
    def __init__(self, debug=False):
        self.api_key = os.getenv("DD_API_KEY")
        self.app_key = os.getenv("DD_APP_KEY")
        self.site = os.getenv("DD_SITE", "datadoghq.com")
        self.base = f"https://api.{self.site}"
        self.debug = debug
        self.headers = {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
            "Content-Type": "application/json",
        }

    def query_metric(self, query: str, since: int, until: int) -> pd.DataFrame:
        """Fetch Datadog metrics as a DataFrame"""

        params = {"query": query, "from": since, "to": until}
        if self.debug:
            print(f"Querying {query} from {since} to {until}")

        url = f"{self.base}/api/v1/query"
        resp = requests.get(url, headers=self.headers, params=params)
        if resp.status_code != 200:
            raise Exception(f"Error: {resp.status_code} - {resp.text}")

        resp.raise_for_status()
        data = resp.json()
        return self._metric_to_dataframe(data)

    def _metric_to_dataframe(self, data: dict) -> pd.DataFrame:
        """
        Convert Datadog time-series JSON response to a clean pandas DataFrame.
        """
        if "series" not in data or not data["series"]:
            print("No series data found in response.")
            return pd.DataFrame()

        rows = []
        for series in data["series"]:
            metric = series.get("metric", "")
            scope = series.get("scope", "")
            host = None

            # Extract host tag if available
            for tag in series.get("tag_set", []):
                if tag.startswith("host:"):
                    host = tag.split(":", 1)[1]

            for point in series.get("pointlist", []):
                if point[1] is not None:  # skip nulls
                    timestamp = point[0]
                    value = point[1]
                    rows.append(
                        {
                            "timestamp": pd.to_datetime(timestamp, unit="ms"),
                            "value": value,
                            "metric": metric,
                            "host": host,
                            "scope": scope,
                        }
                    )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def query_logs(self, since: int, until: int, query: str = "*", limit=1000):
        """Query Datadog logs within a time range and return as a pandas DataFrame."""

        url = f"{self.base}/api/v2/logs/events/search"
        payload = {
            "filter": {
                "from": f"{since}",
                "to": f"{until}",
                "query": query,
            },
            "page": {"limit": limit},
            "sort": "desc",
        }

        if self.debug:
            print(f"Querying logs with query='{query}' from {since} to {until}")

        resp = requests.post(url, headers=self.headers, json=payload)
        if resp.status_code != 200:
            raise Exception(f"Error: {resp.status_code} - {resp.text}")

        data = resp.json()
        if data["data"] != []:
            return self._logs_to_dataframe(data)
        print("No logs found in desired time range")
        return None

    def _logs_to_dataframe(self, data: dict) -> pd.DataFrame:
        """Convert Datadog logs JSON to pandas DataFrame."""
        if not data or "data" not in data:
            return pd.DataFrame()

        rows = []
        for item in data["data"]:
            attrs = item.get("attributes", {})
            ts = attrs.get("timestamp")
            msg = attrs.get("message", "")
            host = attrs.get("host", None)
            service = attrs.get("service", None)
            status = attrs.get("status", None)

            rows.append(
                {
                    "timestamp": pd.to_datetime(ts),
                    "message": msg,
                    "host": host,
                    "service": service,
                    "status": status,
                }
            )

        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
