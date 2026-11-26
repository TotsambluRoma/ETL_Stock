from datetime import UTC, date, datetime
from functools import cache
from tempfile import TemporaryDirectory

import pandas as pd

from .objects import ObjectStore


class QuoteUnavailable(RuntimeError):
    """The provider has no usable response yet."""


@cache
def _yahoo_client():
    # Параллельные процессы используют разные файлы SQLite.
    directory = TemporaryDirectory(prefix="stock-yfinance-", ignore_cleanup_errors=True)
    import yfinance as yf

    yf.set_tz_cache_location(directory.name)
    return yf, directory


def fetch_history(ticker: str, start: date, end: date, timeout: int) -> pd.DataFrame:
    yf, _ = _yahoo_client()
    prices = yf.Ticker(ticker).history(
        start=start.isoformat(), end=end.isoformat(), interval="1d",
        auto_adjust=False, actions=True, repair=False, keepna=True, timeout=timeout,
    )
    if prices is None or prices.empty:
        raise QuoteUnavailable(f"Yahoo Finance не вернул данные для {ticker}.")
    return prices


def raw_key(run_id: str, ticker: str) -> str:
    return f"raw/{ticker}/{run_id}/prices.parquet"


def save_snapshot(
    store: ObjectStore,
    run_id: str,
    ticker: str,
    start: date,
    end: date,
    timeout: int,
) -> tuple[str, dict]:
    key = raw_key(run_id, ticker)
    metadata = store.metadata(key)
    if metadata is None:
        prices = fetch_history(ticker, start, end, timeout)
        store.write_frame(key, prices, immutable=True, metadata={
            "ticker": ticker,
            "run-id": run_id,
            "start": start.isoformat(),
            "end-exclusive": end.isoformat(),
            "rows": str(len(prices)),
            "fetched-at": datetime.now(UTC).isoformat(),
        })
        metadata = store.metadata(key)
    if not metadata or metadata.get("ticker") != ticker:
        raise ValueError(f"Нет метаданных снимка {key}.")
    return key, metadata
