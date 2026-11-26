from datetime import UTC, date, datetime
import os
from uuid import uuid4

import pandas as pd
import psycopg
import pytest
from prometheus_client import CollectorRegistry, generate_latest

from pipeline import db, extract, workflow
from pipeline.config import Instrument
from pipeline.monitoring import PipelineMetrics
from pipeline.objects import ObjectStore
from pipeline.quality import QualityError


pytestmark = pytest.mark.skipif(
    os.environ.get("PIPELINE_INTEGRATION") != "1",
    reason="Нужны PostgreSQL и MinIO с TLS на отдельном тестовом стенде.",
)


def test_full_training_forecast_and_replay(monkeypatch, raw_prices):
    ticker = "QA" + uuid4().hex[:8].upper()
    instrument = Instrument(ticker, "Quality Test", "XNAS", "USD", date(2024, 9, 3), True)
    monkeypatch.setattr(workflow, "read_instruments", lambda: [instrument])
    monkeypatch.setattr(extract, "fetch_history", lambda *args: raw_prices)

    def load(params):
        rid = str(uuid4())
        planned, = workflow.prepare_run(rid, "etl", params)
        extracted = workflow.extract_batch(planned)
        checked = workflow.transform_batch(extracted)
        loaded = workflow.load_batch(checked)
        return rid, loaded

    full = {"mode": "full", "through_date": "2024-12-31"}
    initial, loaded = load(full)
    assert workflow.summarize_run(initial)["status"] == "success"
    store = ObjectStore(workflow.read_settings().bucket)
    pd.testing.assert_frame_equal(store.read_frame(loaded["raw_key"]), raw_prices, check_freq=False)

    train_id = str(uuid4())
    batch, = workflow.prepare_run(train_id, "train", {"through_date": "2024-12-31"})
    metrics = workflow.train_batch(batch)
    assert metrics["test_count"] >= 1 and metrics["mse"] >= 0
    assert workflow.summarize_run(train_id)["status"] == "success"

    class FixedTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2025, 1, 1, 12, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(workflow, "datetime", FixedTime)
    future, latest = load(full)
    forecast = workflow.forecast_batch(latest)
    assert forecast["status"] == "published" and forecast["forecast_date"] == "2025-01-02"
    assert forecast["predicted_close"] > 0
    assert workflow.forecast_batch(latest)["status"] == "already_published"
    assert workflow.summarize_run(future)["status"] == "no_changes"

    class AfterOpening(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2025, 1, 2, 14, 30, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(workflow, "datetime", AfterOpening)
    expired, loaded_too_late = load(full)
    assert workflow.forecast_batch(loaded_too_late)["status"] == "skipped"
    assert workflow.summarize_run(expired)["status"] == "no_changes"

    def no_download(*args):
        pytest.fail("Replay должен работать без запроса к Yahoo Finance.")

    monkeypatch.setattr(extract, "fetch_history", no_download)
    replay, _ = load({"mode": "replay", "snapshot_run_id": initial})
    assert workflow.summarize_run(replay)["status"] == "no_changes"
    with db.connect() as conn:
        assert conn.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()["ssl"]
        rows = conn.execute("SELECT * FROM forecast_comparison WHERE ticker = %s", (ticker,)).fetchall()
        assert len(rows) == 1 and rows[0]["actual_close"] is None
        assert conn.execute("SELECT COUNT(*) AS n FROM daily_prices WHERE instrument_id = %s",
                            (loaded["instrument_id"],)).fetchone()["n"] == len(raw_prices)

    updated = raw_prices.copy()
    changed_date = updated.index[-3]
    updated.loc[changed_date, "Close"] += 0.5
    monkeypatch.setattr(extract, "fetch_history", lambda *args: updated)
    corrected, _ = load(full)
    correction = workflow.summarize_run(corrected)["details"]["tickers"][ticker]
    assert correction["status"] == "success" and correction["corrected"] == 1
    with db.connect() as conn:
        prices = db.read_history(conn, loaded["instrument_id"]).set_index("trade_date")
        features = db.read_features(conn, loaded["instrument_id"], date(2024, 12, 31)).set_index("trade_date")
    expected_average = updated["Close"].rolling(5).mean().loc[changed_date]
    assert prices.loc[changed_date, "close"] == pytest.approx(updated.loc[changed_date, "Close"])
    assert features.loc[changed_date, "sma_5"] == pytest.approx(expected_average)

    corrupt = updated.drop(updated.index[5])
    monkeypatch.setattr(extract, "fetch_history", lambda *args: corrupt)
    rejected_id = str(uuid4())
    bad, = workflow.prepare_run(rejected_id, "etl", full)
    bad = workflow.extract_batch(bad)
    with pytest.raises(QualityError, match="пропусков 1"):
        workflow.run_step("transform", workflow.transform_batch, bad)
    result = workflow.summarize_run(rejected_id)
    assert result["status"] == "failed"
    assert result["details"]["tickers"][ticker]["rejected_rows"] == len(corrupt)
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS n FROM daily_prices WHERE instrument_id = %s",
                            (loaded["instrument_id"],)).fetchone()["n"] == len(raw_prices)
    registry = CollectorRegistry()
    registry.register(PipelineMetrics())
    assert b'stock_pipeline_last_status{operation="etl",status="failed"} 1.0' in generate_latest(registry)


def test_load_rollback_leaves_other_ticker_available(monkeypatch, raw_prices):
    suffix = uuid4().hex[:8].upper()
    good = Instrument("GOOD" + suffix, "Independent Test", "XNAS", "USD", date(2024, 9, 3), True)
    bad = Instrument("BAD" + suffix, "Rollback Test", "XNAS", "USD", date(2024, 9, 3), True)
    monkeypatch.setattr(workflow, "read_instruments", lambda: [good, bad])
    monkeypatch.setattr(extract, "fetch_history", lambda *args: raw_prices)
    run_id = str(uuid4())
    work = workflow.prepare_run(run_id, "etl", {"mode": "full", "through_date": "2024-12-31"})
    first, second = [workflow.transform_batch(workflow.extract_batch(item)) for item in work]
    workflow.load_batch(first)

    store = ObjectStore(workflow.read_settings().bucket)
    key = f"{second['prepared']}/features.parquet"
    invalid = store.read_frame(key)
    invalid.loc[invalid.index[0], "trade_date"] = pd.Timestamp("2025-01-02")
    store.write_frame(key, invalid)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        workflow.run_step("load", workflow.load_batch, second)
    summary = workflow.summarize_run(run_id)
    assert summary["status"] == "partial"
    with db.connect() as conn:
        dates_ok = db.stored_dates(conn, first["instrument_id"])
        dates_failed = db.stored_dates(conn, second["instrument_id"])
    assert len(dates_ok) == len(raw_prices) and not dates_failed
