import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import time
from dotenv import load_dotenv

load_dotenv()


class DDClient:
    def __init__(
        self,
        api_key: str | None = None,
        app_key: str | None = None,
        site: str | None = None,
        debug: bool | None = None,
        config: dict | None = None,
    ):
        """
        Initialize Datadog client.

        Args:
            api_key: Datadog API key.
            app_key: Datadog Application key.
            site: Datadog site (e.g., datadoghq.com, datadoghq.eu).
            debug: Enable debug logging.
            config: Optional dict (from ddpipe.config.load_env()).
        """

        if config:
            self.api_key = api_key or config.get("api_key")
            self.app_key = app_key or config.get("app_key")
            self.site = site or config.get("site")
            if debug is None:
                self.debug = config.get("debug", False)
        else:
            self.api_key = api_key or os.getenv("DD_API_KEY")
            self.app_key = app_key or os.getenv("DD_APP_KEY")
            self.site = site or os.getenv("DD_SITE", "datadoghq.com")
            self.debug = (
                debug
                if debug is not None
                else (os.getenv("DD_DEBUG", "false").lower() == "true")
            )

        self.base = f"https://api.{self.site}"
        self.headers = {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
            "Content-Type": "application/json",
        }

        if self.debug:
            print(f"[DDClient] Initialized for site={self.site}")

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

    def correlate_metrics_logs(
        self,
        metric_query: str,
        log_query: str = "*",
        until: int = int(time.time()),
        since: int = int(time.time()) - (3600 * 2),  # default 2 hours back,
        time_tolerance_sec: int = 60,
    ):
        """
        Correlate metrics and logs over the same time window.

        Args:
            metric_query: Datadog metric query (e.g., 'avg:system.cpu.user{*} by {host}')
            log_query: Log search query (e.g., 'service:system')
            since, until: epoch timestamps (default: last 1h)
            time_tolerance_sec: max time gap between log and metric samples for merge
        """
        metrics_df = self.query_metric(metric_query, since, until)
        logs_df = self.query_logs(since, until, query=log_query)

        if metrics_df.empty or logs_df.empty:
            print("One or both datasets are empty.")
            return pd.DataFrame()

        metrics_df = metrics_df.sort_values("timestamp")
        logs_df = logs_df.sort_values("timestamp")

        metrics_df["timestamp"] = pd.to_datetime(metrics_df["timestamp"])
        logs_df["timestamp"] = pd.to_datetime(logs_df["timestamp"])

        # converting from datetime64[ns, UTC] to datetime64[ns] (logs return in UTC)
        logs_df["timestamp"] = pd.to_datetime(logs_df.timestamp).dt.tz_localize(None)

        # 4️⃣ Merge by nearest timestamp and same host
        merged = pd.merge_asof(
            logs_df,
            metrics_df,
            on="timestamp",
            by="host",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=time_tolerance_sec),
        )

        return merged
