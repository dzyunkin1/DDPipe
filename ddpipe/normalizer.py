import pandas as pd


def normalize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure timestamps and column names are standardized."""
    if df.empty:
        return df
    df = df.sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def normalize_logs(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten log attributes, clean messages."""
    if df.empty:
        return df
    df = df.sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # converting from datetime64[ns, UTC] to datetime64[ns] (logs return in UTC)
    df["timestamp"] = pd.to_datetime(df.timestamp).dt.tz_localize(None)
    return df


def correlate(df_metrics, df_logs, window="1min"):
    """Join metrics and logs on a rolling window."""
    df_metrics = normalize_metrics(df_metrics)
    df_logs = normalize_logs(df_logs)
    return pd.merge_asof(
        df_metrics.sort_values("timestamp"),
        df_logs.sort_values("timestamp"),
        on="timestamp",
        tolerance=pd.Timedelta(window),
        direction="nearest",
    )
