import numpy as np
import pandas as pd

from .config import FEATURES, PRICE_FIELDS
from .market import NY, schedule
from .quality import OPTIONAL, REQUIRED, QualityError


def normalize_prices(raw: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    names = {" ".join(name.strip().lower().split()): name for name in raw.columns}
    if any(field not in names for field in REQUIRED):
        raise QualityError("В ответе отсутствуют обязательные поля OHLCV.")

    dates = pd.to_datetime(raw.index, errors="coerce")
    if not isinstance(dates, pd.DatetimeIndex) or dates.isna().any():
        raise QualityError("Не удалось прочитать торговую дату.")
    if dates.tz is not None:
        dates = dates.tz_convert(NY).tz_localize(None)
    frame = pd.DataFrame({"trade_date": dates.normalize().astype("datetime64[ns]")})
    for field in PRICE_FIELDS:
        name = names.get(field.replace("_", " "))
        if name is None:
            frame[field] = np.nan
            continue
        original = raw[name].reset_index(drop=True)
        frame[field] = pd.to_numeric(original, errors="coerce")
        if field in OPTIONAL and (original.notna() & frame[field].isna()).any():
            raise QualityError(f"Поле {field} содержит значение, которое не является числом.")

    unique = frame.drop_duplicates()
    if unique["trade_date"].duplicated().any():
        raise QualityError("В снимке разные котировки для одной даты.")
    return unique.sort_values("trade_date").reset_index(drop=True), len(frame) - len(unique)


def calculate_features(prices: pd.DataFrame, sessions: pd.DatetimeIndex) -> pd.DataFrame:
    history = prices.set_index("trade_date").reindex(sessions)
    close = history["close"]
    returns = close.div(close.shift(1)).sub(1)
    features = pd.DataFrame({
        "close": close,
        "return_1d": returns,
        "sma_5": close.rolling(5).mean(),
        "sma_20": close.rolling(20).mean(),
        "avg_volume_20": history["volume"].rolling(20).mean(),
        "volatility_20": returns.rolling(20).std(ddof=0),
    }, index=sessions)
    return features.dropna(subset=list(FEATURES)).rename_axis("trade_date").reset_index()


def prepare_changes(prices: pd.DataFrame, existing: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    incoming = prices.set_index("trade_date")
    previous = existing.set_index("trade_date").reindex(incoming.index)
    same = incoming.eq(previous) | (incoming.isna() & previous.isna())
    changed = incoming.loc[~same.all(axis=1)]
    if changed.empty:
        return changed.reset_index(), pd.DataFrame(columns=["trade_date", *FEATURES]), 0

    history = pd.concat([
        existing.loc[~existing["trade_date"].isin(prices["trade_date"])], prices
    ], ignore_index=True).sort_values("trade_date")
    sessions = schedule(history["trade_date"].min().date(), history["trade_date"].max().date()).index
    first_changed = changed.index.min()
    start = max(0, sessions.searchsorted(first_changed) - 20)
    features = calculate_features(history, sessions[start:])
    features = features.loc[features["trade_date"] >= first_changed].reset_index(drop=True)
    new_count = int((~changed.index.isin(existing["trade_date"])).sum())
    return changed.reset_index(), features, new_count
