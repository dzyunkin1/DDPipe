import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class DDClient:
    def __init__(self):
        self.api_key = os.getenv("DD_API_KEY")
        self.app_key = os.getenv("DD_APP_KEY")
        self.site = os.getenv("DD_SITE", "datadoghq.com")
        self.base = f"https://api.{self.site}/api/v1/query"

    def query(self, query: str, since: int, until: int) -> pd.DataFrame:
        """Fetch Datadog metrics as a DataFrame"""
        headers = {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
        }
        params = {"query": query, "from": since, "to": until}
        resp = requests.get(self.base, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        return self._to_dataframe(data)

    def _to_dataframe(self, data: dict) -> pd.DataFrame:
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
                    rows.append(
                        {
                            "timestamp": pd.to_datetime(point[0], unit="ms"),
                            "value": point[1],
                            "metric": metric,
                            "host": host,
                            "scope": scope,
                        }
                    )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("timestamp").reset_index(drop=True)

        return df
