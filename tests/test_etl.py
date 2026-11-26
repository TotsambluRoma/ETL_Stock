from datetime import UTC, date, datetime

import pandas as pd
import pytest

from pipeline import extract
from pipeline.market import last_closed, next_session, request_window, schedule
from pipeline.quality import QualityError, feature_checks, price_checks
from pipeline.transform import calculate_features, normalize_prices, prepare_changes


def test_calendar_with_holiday_and_short_session():
    assert last_closed(datetime(2024, 11, 29, 17, 59, tzinfo=UTC)) == date(2024, 11, 27)
    assert last_closed(datetime(2024, 11, 29, 18, tzinfo=UTC)) == date(2024, 11, 29)
    assert next_session(date(2024, 12, 31))[0] == date(2025, 1, 2)


def test_incremental_overlap_and_old_gap():
    start, end = date(2024, 12, 2), date(2024, 12, 20)
    days = list(schedule(start, end).index.date)
    assert request_window(start, end, days[:-2], "incremental", 5) == (days[-7], date(2024, 12, 21))
    missing = days[3]
    assert request_window(start, end, [d for d in days if d != missing], "incremental", 5)[0] == missing
    assert request_window(start, end, days[-3:], "incremental", 5)[0] == start
    assert request_window(start, end, days[:-2], "full", 5)[0] == start
    with pytest.raises(ValueError, match="нет завершённых"):
        request_window(date(2024, 12, 21), date(2024, 12, 22), [], "full", 5)


class MemoryStore:
    def __init__(self):
        self.metadata_by_key = {}
        self.frames = {}

    def metadata(self, key):
        return self.metadata_by_key.get(key)

    def write_frame(self, key, prices, *, metadata, immutable):
        assert immutable
        self.metadata_by_key[key] = metadata
        self.frames[key] = prices


def test_snapshot_is_reused_on_retry(monkeypatch, raw_prices):
    calls = []

    def fetch(ticker, start, end, timeout):
        calls.append((ticker, start, end, timeout))
        return raw_prices

    monkeypatch.setattr(extract, "fetch_history", fetch)
    store = MemoryStore()
    args = (store, "batch-1", "AAPL", date(2024, 9, 3), date(2025, 1, 1), 20)
    key, metadata = extract.save_snapshot(*args)
    assert extract.save_snapshot(*args) == (key, metadata)
    assert calls == [args[2:]]
    assert key == "raw/AAPL/batch-1/prices.parquet"
    assert metadata["end-exclusive"] == "2025-01-01"
    pd.testing.assert_frame_equal(store.frames[key], raw_prices)


def test_yahoo_query_arguments_and_empty_response(monkeypatch):
    import yfinance

    cache_paths = []

    class Stock:
        def history(self, **options):
            assert len(cache_paths) == 1
            assert options["start"] == "2024-09-03" and options["end"] == "2025-01-01"
            assert options["interval"] == "1d" and options["actions"] and options["keepna"]
            assert not options["auto_adjust"] and not options["repair"]
            return pd.DataFrame()

    monkeypatch.setattr(yfinance, "Ticker", lambda ticker: Stock())
    monkeypatch.setattr(yfinance, "set_tz_cache_location", cache_paths.append)
    extract._yahoo_client.cache_clear()
    try:
        for ticker in ("AAPL", "MSFT"):
            with pytest.raises(extract.QuoteUnavailable):
                extract.fetch_history(ticker, date(2024, 9, 3), date(2025, 1, 1), 20)
    finally:
        extract._yahoo_client.cache_clear()


def test_duplicate_and_conflict(raw_prices):
    duplicated = pd.concat([raw_prices, raw_prices.iloc[[0]]])
    prices, count = normalize_prices(duplicated)
    assert count == 1 and len(prices) == len(raw_prices)
    assert prices["adj_close"].isna().all()
    conflicting = raw_prices.iloc[[0]].assign(Close=900)
    with pytest.raises(QualityError, match="разные котировки"):
        normalize_prices(pd.concat([raw_prices, conflicting]))


@pytest.mark.parametrize("column", ["Volume", "Close", "Adj Close"])
def test_invalid_input_is_rejected(raw_prices, column):
    raw = raw_prices.copy()
    if column == "Volume":
        raw[column] = raw[column].astype(float)
        raw.loc[raw.index[0], column] = 0.5
    elif column == "Close":
        raw.loc[raw.index[0], column] = -1
    else:
        raw[column] = "не число"
    with pytest.raises(QualityError):
        prices, _ = normalize_prices(raw)
        price_checks(prices, date(2024, 9, 3), date(2025, 1, 1))


def test_market_gap_is_not_silently_filled(raw_prices):
    prices, _ = normalize_prices(raw_prices.drop(raw_prices.index[14]))
    with pytest.raises(QualityError, match="пропусков 1"):
        price_checks(prices, date(2024, 9, 3), date(2025, 1, 1))
    features = calculate_features(prices, raw_prices.index)
    assert features["trade_date"].iloc[0] == raw_prices.index[35]


def test_recalculate_after_correction_without_future_leakage(raw_prices):
    prices, _ = normalize_prices(raw_prices)
    old_features = calculate_features(prices, raw_prices.index)
    unchanged, features, added = prepare_changes(prices, prices)
    assert unchanged.empty and features.empty and added == 0
    assert len(old_features) == len(prices) - 20

    corrected = prices.iloc[[25]].copy()
    corrected["close"] += 0.5
    changes, features, added = prepare_changes(corrected, prices)
    assert len(changes) == 1 and added == 0
    assert features["trade_date"].iloc[0] == corrected["trade_date"].iloc[0]
    assert len(features) == len(prices) - 25
    assert feature_checks(features)
    assert old_features.loc[5, "sma_20"] != features.loc[0, "sma_20"]
    earlier = calculate_features(prices.iloc[:25], raw_prices.index[:25])
    pd.testing.assert_frame_equal(earlier, old_features.iloc[:5].reset_index(drop=True))
