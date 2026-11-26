import csv
from dataclasses import dataclass
from datetime import date
import os
from pathlib import Path
import re

import yaml


CONFIG_DIR = Path(os.environ.get("PIPELINE_CONFIG", Path(__file__).resolve().parents[1] / "config"))
FEATURES = ("close", "return_1d", "sma_5", "sma_20", "avg_volume_20", "volatility_20")
PRICE_FIELDS = ("open", "high", "low", "close", "volume", "adj_close", "dividends", "stock_splits")


@dataclass(frozen=True)
class Settings:
    schedule: str
    overlap_sessions: int
    request_timeout_seconds: int
    retry_count: int
    retry_delay_minutes: int
    bucket: str


@dataclass(frozen=True)
class Instrument:
    ticker: str
    company_name: str
    exchange: str
    currency: str
    start_date: date
    active: bool


def read_settings() -> Settings:
    with (CONFIG_DIR / "settings.yaml").open(encoding="utf-8") as file:
        settings = Settings(**yaml.safe_load(file))
    if settings.overlap_sessions < 1 or settings.request_timeout_seconds < 1:
        raise ValueError("Перекрытие дат и время ожидания должны быть положительными.")
    if settings.retry_count < 0 or settings.retry_delay_minutes < 1:
        raise ValueError("Неверные параметры повторных попыток.")
    return settings


def read_instruments() -> list[Instrument]:
    required = {"ticker", "company_name", "exchange", "currency", "start_date", "active"}
    instruments = []
    seen = set()
    with (CONFIG_DIR / "tickers.csv").open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        fields = reader.fieldnames or []
        if len(fields) != len(required) or set(fields) != required:
            raise ValueError("У tickers.csv неверный заголовок.")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Неверное число полей в строке {reader.line_num} tickers.csv.")
            ticker, name = row["ticker"].strip().upper(), row["company_name"].strip()
            exchange, currency = row["exchange"].strip(), row["currency"].strip()
            active = row["active"].strip().lower()
            if not re.fullmatch(r"[A-Z0-9][A-Z0-9.-]{0,14}", ticker) or not name:
                raise ValueError(f"Некорректный инструмент в строке {reader.line_num}.")
            if exchange != "XNAS" or currency != "USD":
                raise ValueError(f"{ticker}: этот проект работает с XNAS и USD.")
            if ticker in seen or active not in {"true", "false"}:
                raise ValueError(f"{ticker}: повтор тикера или неверное значение active.")
            seen.add(ticker)
            instruments.append(Instrument(
                ticker, name, exchange, currency, date.fromisoformat(row["start_date"]), active == "true"
            ))
    return instruments
