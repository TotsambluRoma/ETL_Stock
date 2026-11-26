from datetime import UTC, date, datetime, timedelta
from email.message import EmailMessage
import logging
import os
import smtplib
from uuid import UUID

from . import db
from .config import FEATURES, read_instruments, read_settings
from .extract import raw_key, save_snapshot
from .market import last_closed, next_session, request_window
from .objects import ObjectStore
from .quality import QualityError, feature_checks, price_checks
from .transform import normalize_prices, prepare_changes


LOG = logging.getLogger(__name__)
GOOD = {"success", "no_changes"}


def prepare_run(run_id: str, operation: str, params: dict) -> list[dict]:
    if operation not in {"etl", "train"}:
        raise ValueError("Неизвестная операция.")
    mode = params.get("mode", "incremental") if operation == "etl" else "train"
    if operation == "etl" and mode not in {"full", "incremental", "replay"}:
        raise ValueError("Неизвестный режим запуска.")
    snapshot = params.get("snapshot_run_id")
    from_date = params.get("from_date")
    through_date = params.get("through_date")
    if mode == "replay" and (not snapshot or from_date or through_date):
        raise ValueError("Для replay укажите ID предыдущего снимка без диапазона дат.")
    if mode != "replay" and snapshot:
        raise ValueError("Для snapshot_run_id нужен режим replay.")
    if snapshot:
        snapshot = str(UUID(snapshot))
    requested_start = date.fromisoformat(from_date) if from_date else None
    requested_end = date.fromisoformat(through_date) if through_date else None
    selected = params.get("ticker")
    instruments = read_instruments()
    targets = [i for i in instruments if i.active and (selected is None or i.ticker == selected)]
    if not targets:
        raise ValueError("Нет активного тикера для обработки.")
    details = {"mode": mode, "tickers": {i.ticker: {"status": "pending"} for i in targets}}
    batch = []
    with db.connect() as conn:
        run = db.start_run(conn, run_id, operation, details)
        if run["finished_at"] is not None:
            raise ValueError("Для повторной обработки запустите новый DAG или replay.")
        cutoff = last_closed(run["started_at"])
        through = min(cutoff, requested_end or cutoff)
        ids = {i.ticker: db.instrument_id(conn, i) for i in instruments}
        for i in targets:
            start = max(i.start_date, requested_start or i.start_date)
            batch.append({
                "run_id": run_id, "ticker": i.ticker, "instrument_id": ids[i.ticker], "mode": mode,
                "start": start.isoformat(), "through": through.isoformat(), "snapshot_run_id": snapshot,
                "activate": params.get("activate", True),
            })
    LOG.info("run_id=%s: %s, режим %s, активных тикеров %d", run_id, operation, mode, len(batch))
    return batch


def extract_batch(item: dict) -> dict:
    settings = read_settings()
    store = ObjectStore(settings.bucket)
    store.ensure_bucket()
    if item["mode"] == "replay":
        key = raw_key(item["snapshot_run_id"], item["ticker"])
        metadata = store.metadata(key)
        if not metadata or metadata.get("ticker") != item["ticker"]:
            raise QualityError(f"Снимок не найден: {key}")
    else:
        with db.connect() as conn:
            dates = db.stored_dates(conn, item["instrument_id"])
        start, end = request_window(
            date.fromisoformat(item["start"]), date.fromisoformat(item["through"]),
            dates, item["mode"], settings.overlap_sessions,
        )
        key, metadata = save_snapshot(
            store, item["run_id"], item["ticker"], start, end, settings.request_timeout_seconds
        )
    with db.connect() as conn:
        db.update_ticker(conn, item["run_id"], item["ticker"], {
            "raw_key": key, "source_rows": int(metadata["rows"]),
            "start": metadata["start"], "end_exclusive": metadata["end-exclusive"],
        })
    return {**item, "raw_key": key, "request_start": metadata["start"],
            "request_end": metadata["end-exclusive"], "source_rows": int(metadata["rows"])}


def transform_batch(item: dict) -> dict:
    store = ObjectStore(read_settings().bucket)
    raw = store.read_frame(item["raw_key"])
    report = f"checks/{item['run_id']}/{item['ticker']}.json"
    start, end = date.fromisoformat(item["request_start"]), date.fromisoformat(item["request_end"])
    try:
        prices, duplicates = normalize_prices(raw)
        price_rules = price_checks(prices, start, end)
        with db.connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            history = db.read_history(conn, item["instrument_id"])
            revision = db.history_revision(conn, item["instrument_id"])
        changed, features, added = prepare_changes(prices, history)
        feature_rules = feature_checks(features)
    except QualityError as error:
        with db.connect() as conn:
            db.update_ticker(conn, item["run_id"], item["ticker"], {
                "rejected_rows": len(raw), "quality_report": report,
            })
        store.write_json(report, {"passed": False, "error": str(error), "rejected_rows": len(raw)})
        raise

    if duplicates:
        LOG.warning("run_id=%s ticker=%s: удалены одинаковые повторы: %s",
                    item["run_id"], item["ticker"], duplicates)
    store.write_json(report, {
        "passed": True, "price_rules": price_rules, "feature_rules": feature_rules,
        "received_rows": len(raw), "identical_duplicates": duplicates,
        "changed_quotes": len(changed), "prepared_features": len(features),
    })
    prefix = f"prepared/{item['run_id']}/{item['ticker']}"
    store.write_frame(f"{prefix}/prices.parquet", changed)
    store.write_frame(f"{prefix}/features.parquet", features)
    return {**item, "prepared": prefix, "revision": revision, "added": added,
            "quality_report": report, "identical_duplicates": duplicates}


def load_batch(item: dict) -> dict:
    store = ObjectStore(read_settings().bucket)
    prices = store.read_frame(f"{item['prepared']}/prices.parquet")
    features = store.read_frame(f"{item['prepared']}/features.parquet")
    with db.connect() as conn:
        db.lock_instrument(conn, item["instrument_id"])
        state = db.get_run(conn, item["run_id"])["details"]["tickers"][item["ticker"]]
        if state["status"] in GOOD:
            return item
        if db.history_revision(conn, item["instrument_id"]) != item["revision"]:
            raise QualityError("Котировки после проверки изменились. Запустите обработку заново.")
        db.write_batch(conn, item["instrument_id"], item["run_id"], item["raw_key"], prices, features)
        db.update_ticker(conn, item["run_id"], item["ticker"], {
            "stage": "load", "status": "no_changes" if prices.empty else "success",
            "added": item["added"], "corrected": len(prices) - item["added"],
            "features": len(features), "rejected_rows": 0,
            "last_session": (date.fromisoformat(item["request_end"]) - timedelta(days=1)).isoformat(),
            "quality_report": item["quality_report"], "identical_duplicates": item["identical_duplicates"],
            "error": "",
        })
    return item


def forecast_batch(item: dict):
    from .model import predict

    now = datetime.now(UTC)
    as_of = date.fromisoformat(item["request_end"]) - timedelta(days=1)
    target_date, opening = next_session(as_of)
    forecast = {"status": "skipped", "forecast_date": target_date.isoformat()}
    if item["mode"] == "replay" or as_of != last_closed(now) or now >= opening:
        forecast["reason"] = "Исторический срез или целевая сессия уже открылась."
    else:
        with db.connect() as conn:
            chosen = db.forecast_input(conn, item["instrument_id"], as_of)
        if chosen is None:
            forecast["reason"] = "Нет полного окна признаков или активной модели."
        else:
            store = ObjectStore(read_settings().bucket)
            inputs = {feature: chosen[feature] for feature in FEATURES}
            price = predict(store.get(store.key(chosen["artifact_uri"])), inputs, as_of)
            if price is None:
                forecast["reason"] = "Модель обучена на более поздних ценах."
            else:
                with db.connect() as conn:
                    db.lock_instrument(conn, item["instrument_id"])
                    issued_at = datetime.now(UTC)
                    if issued_at >= opening:
                        forecast["reason"] = "Целевая сессия уже открылась."
                    else:
                        status, final_price = db.write_forecast(
                            conn, item["instrument_id"], target_date, chosen["model_version"], as_of,
                            price, inputs, issued_at,
                        )
                        forecast.update(status=status, predicted_close=final_price,
                                        version=str(chosen["model_version"]))
    with db.connect() as conn:
        db.update_ticker(conn, item["run_id"], item["ticker"], {"forecast": forecast})
    return forecast


def train_batch(item: dict):
    from .model import fit, training_examples

    through = date.fromisoformat(item["through"])
    store = ObjectStore(read_settings().bucket)
    store.ensure_bucket()
    prefix = f"models/{item['ticker']}/{item['run_id']}"
    dataset, artifact = f"{prefix}/train.parquet", f"{prefix}/weights.pt"
    if store.metadata(dataset) is None:
        with db.connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            prices = db.read_history(conn, item["instrument_id"])
            features = db.read_features(conn, item["instrument_id"], through)
        examples = training_examples(prices, features, through)
        store.write_frame(dataset, examples, immutable=True)
    weights, metrics = fit(store.read_frame(dataset), through)
    store.put(artifact, weights, immutable=True)
    with db.connect() as conn:
        db.register_model(conn, item["instrument_id"], item["run_id"], store.uri(artifact),
                          store.uri(dataset), metrics, item["activate"])
        db.update_ticker(conn, item["run_id"], item["ticker"], {
            "stage": "train", "status": "success", "model_version": item["run_id"],
            "active": item["activate"], "error": "",
            "dataset_uri": store.uri(dataset), "artifact_uri": store.uri(artifact), **metrics,
        })
    return metrics


def run_step(name, function, item: dict):
    rid, ticker = item["run_id"], item["ticker"]
    LOG.info("run_id=%s ticker=%s: %s", rid, ticker, name)
    try:
        result = function(item)
    except Exception as error:
        LOG.exception("run_id=%s ticker=%s: ошибка %s", rid, ticker, name)
        reason = f"{type(error).__name__}: {error}"[:500]
        if name == "forecast":
            update = {"forecast": {"status": "error", "reason": reason}}
        else:
            update = {"status": "failed", "stage": name, "error": reason}
        try:
            with db.connect() as conn:
                current = db.get_run(conn, rid)["details"]["tickers"][ticker]
                if name == "forecast" or current["status"] not in GOOD:
                    db.update_ticker(conn, rid, ticker, update)
        except Exception:
            LOG.warning("run_id=%s ticker=%s: статус ошибки не сохранён", rid, ticker, exc_info=True)
        raise
    if name == "forecast" and result["status"] == "skipped":
        LOG.warning("run_id=%s ticker=%s: прогноз пропущен: %s", rid, ticker, result["reason"])
    LOG.info("run_id=%s ticker=%s: %s завершён", rid, ticker, name)
    return result


def summarize_run(run_id: str) -> dict:
    with db.connect() as conn:
        run = db.get_run(conn, run_id)
        if run is None:
            raise ValueError("Запуск не был зарегистрирован.")
        if run["finished_at"] is None:
            details = run["details"]
            for result in details["tickers"].values():
                if result["status"] == "pending":
                    result.update(status="failed", error="Задача не завершилась. См. журнал Airflow.")
            states = [result["status"] for result in details["tickers"].values()]
            if all(state in GOOD for state in states):
                status = "success" if "success" in states else "no_changes"
            else:
                status = "partial" if any(state in GOOD for state in states) else "failed"
            run = db.finish_run(conn, run_id, status, details) or db.get_run(conn, run_id)
    LOG.info("run_id=%s operation=%s status=%s", run_id, run["operation"], run["status"])
    return run


def notify(run: dict):
    if run["details"].get("notified") or not os.environ.get("SMTP_HOST"):
        return
    message = EmailMessage()
    message["From"] = os.environ["MAIL_FROM"]
    message["To"] = os.environ["MAIL_TO"]
    message["Subject"] = f"Stock pipeline: {run['operation']} / {run['status']}"
    message["Message-ID"] = f"<{run['run_id']}@stock-pipeline.local>"
    lines = [f"Запуск {run['run_id']}: {run['status']}"]
    for ticker, result in run["details"]["tickers"].items():
        lines.append(f"{ticker}: {result['status']}. {result.get('error', '')}")
        forecast = result.get("forecast")
        if forecast:
            lines.append(f"  Прогноз: {forecast['status']}. {forecast.get('reason', '')}")
    message.set_content("\n".join(lines))
    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ.get("SMTP_PORT", "1025")), timeout=10) as smtp:
        smtp.send_message(message)
    with db.connect() as conn:
        db.mark_notified(conn, run["run_id"])
