from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as market_calendars


NY = ZoneInfo("America/New_York")


@lru_cache(maxsize=32)
def schedule(start: date, end: date) -> pd.DataFrame:
    return market_calendars.get_calendar("NASDAQ").schedule(start_date=start, end_date=end)


def last_closed(now: datetime) -> date:
    if now.tzinfo is None:
        raise ValueError("Для времени запуска нужен часовой пояс.")
    today = now.astimezone(NY).date()
    days = schedule(today - timedelta(days=31), today)
    complete = days.loc[days["market_close"] <= now.astimezone(UTC)]
    if complete.empty:
        raise ValueError("Не найдена завершённая сессия NASDAQ.")
    return complete.index[-1].date()


def next_session(trade_date: date) -> tuple[date, datetime]:
    days = schedule(trade_date + timedelta(days=1), trade_date + timedelta(days=31))
    if days.empty:
        raise ValueError("Не найдена следующая сессия NASDAQ.")
    return days.index[0].date(), days["market_open"].iloc[0].to_pydatetime()


def request_window(
    start: date,
    end: date,
    stored_dates: list[date],
    mode: str,
    overlap: int,
) -> tuple[date, date]:
    """Return an inclusive first date and an exclusive end for history()."""
    days = list(schedule(start, end).index.date)
    if not days:
        raise ValueError("В выбранном периоде нет завершённых сессий NASDAQ.")
    first = days[0]
    if mode == "incremental":
        inside = set(stored_dates).intersection(days)
        ordered = sorted(inside)
        if len(ordered) >= overlap:
            missing = next((day for day in days if day not in inside), days[-1])
            first = min(ordered[-overlap], missing)
    return first, days[-1] + timedelta(days=1)
