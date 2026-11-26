from dataclasses import asdict
import os

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import FEATURES, PRICE_FIELDS, Instrument


def connect():
    return psycopg.connect(
        host=os.environ["PIPELINE_DB_HOST"],
        port=os.environ.get("PIPELINE_DB_PORT", "5432"),
        dbname=os.environ.get("PIPELINE_DB_NAME", "market"),
        user=os.environ.get("PIPELINE_DB_USER", "market"),
        password=os.environ["PIPELINE_DB_PASSWORD"],
        sslmode="verify-full",
        sslrootcert=os.environ["PIPELINE_CA"],
        connect_timeout=8,
        row_factory=dict_row,
    )


def instrument_id(conn, instrument: Instrument) -> int:
    return conn.execute(
        """
        INSERT INTO instruments (ticker, company_name, exchange, currency, start_date, active)
        VALUES (%(ticker)s, %(company_name)s, %(exchange)s, %(currency)s, %(start_date)s, %(active)s)
        ON CONFLICT (exchange, ticker) DO UPDATE SET
            company_name = EXCLUDED.company_name, currency = EXCLUDED.currency,
            start_date = EXCLUDED.start_date, active = EXCLUDED.active
        RETURNING instrument_id
        """, asdict(instrument),
    ).fetchone()["instrument_id"]


def start_run(conn, run_id, operation, details):
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, operation, status, details)
        VALUES (%s, %s, 'running', %s)
        ON CONFLICT (run_id) DO NOTHING
        """, (run_id, operation, Jsonb(details)),
    )
    return get_run(conn, run_id)


def get_run(conn, run_id):
    return conn.execute("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,)).fetchone()


def update_ticker(conn, run_id, ticker, result):
    conn.execute(
        """
        UPDATE pipeline_runs
        SET details = jsonb_set(details, ARRAY['tickers', %s]::text[],
            COALESCE(details #> ARRAY['tickers', %s]::text[], '{}'::jsonb) || %s, true)
        WHERE run_id = %s
        """, (ticker, ticker, Jsonb(result), run_id),
    )


def finish_run(conn, run_id, status, details):
    return conn.execute(
        """
        UPDATE pipeline_runs SET status = %s, details = %s, finished_at = now()
        WHERE run_id = %s AND finished_at IS NULL
        RETURNING *
        """, (status, Jsonb(details), run_id),
    ).fetchone()


def mark_notified(conn, run_id):
    conn.execute(
        "UPDATE pipeline_runs SET details = details || '{\"notified\": true}'::jsonb WHERE run_id = %s",
        (run_id,),
    )


def stored_dates(conn, instrument):
    return [row["trade_date"] for row in conn.execute(
        "SELECT trade_date FROM daily_prices WHERE instrument_id = %s ORDER BY trade_date",
        (instrument,),
    )]


def read_history(conn, instrument) -> pd.DataFrame:
    fields = ("trade_date", *PRICE_FIELDS)
    rows = conn.execute(
        f"SELECT {', '.join(fields)} FROM daily_prices WHERE instrument_id = %s ORDER BY trade_date",
        (instrument,),
    ).fetchall()
    history = pd.DataFrame(rows, columns=fields)
    history["trade_date"] = pd.to_datetime(history["trade_date"])
    return history


def history_revision(conn, instrument) -> str:
    return conn.execute(
        """
        SELECT md5(COALESCE(string_agg(trade_date::text || ':' || run_id::text, ',' ORDER BY trade_date),
                            '')) AS version
        FROM daily_prices WHERE instrument_id = %s
        """, (instrument,),
    ).fetchone()["version"]


def lock_instrument(conn, instrument):
    conn.execute("SELECT instrument_id FROM instruments WHERE instrument_id = %s FOR UPDATE", (instrument,))


def _price_rows(instrument, run_id, raw_key, prices):
    for row in prices.itertuples(index=False):
        numbers = [None if pd.isna(row[i]) else float(row[i]) for i in range(1, len(row))]
        numbers[PRICE_FIELDS.index("volume")] = int(row.volume)
        yield instrument, row.trade_date.date(), *numbers, run_id, raw_key


def write_batch(conn, instrument, run_id, raw_key, prices, features):
    price_fields = ", ".join(PRICE_FIELDS)
    feature_fields = ", ".join(FEATURES)
    with conn.cursor() as cursor:
        if not prices.empty:
            updates = ", ".join(f"{field} = EXCLUDED.{field}" for field in PRICE_FIELDS)
            values = ", ".join(["%s"] * (len(PRICE_FIELDS) + 4))
            old = ", ".join(f"daily_prices.{field}" for field in PRICE_FIELDS)
            new = ", ".join(f"EXCLUDED.{field}" for field in PRICE_FIELDS)
            cursor.executemany(
                f"""
                INSERT INTO daily_prices (instrument_id, trade_date, {price_fields}, run_id, raw_key)
                VALUES ({values})
                ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
                    {updates}, run_id = EXCLUDED.run_id, raw_key = EXCLUDED.raw_key
                WHERE ({old}) IS DISTINCT FROM ({new})
                """, _price_rows(instrument, run_id, raw_key, prices[["trade_date", *PRICE_FIELDS]]),
            )
        if not features.empty:
            updates = ", ".join(f"{field} = EXCLUDED.{field}" for field in FEATURES)
            values = ", ".join(["%s"] * (len(FEATURES) + 2))
            cursor.executemany(
                f"""
                INSERT INTO daily_features (instrument_id, trade_date, {feature_fields})
                VALUES ({values})
                ON CONFLICT (instrument_id, trade_date) DO UPDATE SET {updates}
                """, (
                    (instrument, row[0].date(), *(float(value) for value in row[1:]))
                    for row in features[["trade_date", *FEATURES]].itertuples(index=False, name=None)
                ),
            )


def read_features(conn, instrument, through):
    fields = ("trade_date", *FEATURES)
    rows = conn.execute(
        f"""SELECT {', '.join(fields)} FROM daily_features
            WHERE instrument_id = %s AND trade_date <= %s ORDER BY trade_date""",
        (instrument, through),
    ).fetchall()
    frame = pd.DataFrame(rows, columns=fields)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def register_model(conn, instrument, version, artifact, dataset, metrics, activate):
    lock_instrument(conn, instrument)
    conn.execute(
        """
        INSERT INTO model_versions
            (instrument_id, model_version, artifact_uri, dataset_uri, mse_test, mse_baseline)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, model_version) DO NOTHING
        """, (instrument, version, artifact, dataset, metrics["mse"], metrics["mse_baseline"]),
    )
    if activate:
        conn.execute(
            "UPDATE model_versions SET is_active = false WHERE instrument_id = %s AND is_active",
            (instrument,),
        )
        conn.execute(
            "UPDATE model_versions SET is_active = true WHERE instrument_id = %s AND model_version = %s",
            (instrument, version),
        )


def forecast_input(conn, instrument, as_of):
    fields = ", ".join(f"f.{field}" for field in FEATURES)
    return conn.execute(
        f"""
        SELECT {fields}, m.model_version, m.artifact_uri
        FROM daily_features AS f
        JOIN model_versions AS m ON m.instrument_id = f.instrument_id AND m.is_active
        WHERE f.instrument_id = %s AND f.trade_date = %s
        """, (instrument, as_of),
    ).fetchone()


def write_forecast(conn, instrument, target, version, as_of, prediction, inputs, timestamp):
    # Recompute/correction of the input does not change what was already issued.
    row = conn.execute(
        """
        INSERT INTO daily_forecasts
            (instrument_id, forecast_date, model_version, as_of_date, predicted_close, input_snapshot, issued_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, forecast_date, model_version) DO NOTHING
        RETURNING predicted_close
        """, (instrument, target, version, as_of, prediction, Jsonb(inputs), timestamp),
    ).fetchone()
    if row is not None:
        return "published", row["predicted_close"]
    previous = conn.execute(
        """SELECT predicted_close FROM daily_forecasts
           WHERE instrument_id = %s AND forecast_date = %s AND model_version = %s""",
        (instrument, target, version),
    ).fetchone()
    return "already_published", previous["predicted_close"]
