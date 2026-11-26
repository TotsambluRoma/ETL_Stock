from datetime import date

import numpy as np
import pandas as pd
import pytest

from pipeline.market import schedule


@pytest.fixture
def raw_prices():
    start, end = date(2024, 9, 3), date(2024, 12, 31)
    dates = schedule(start, end).index
    close = 100 + np.arange(len(dates)) / 9 + np.sin(np.arange(len(dates)) / 4)
    return pd.DataFrame({
        "Open": close - 0.1, "High": close + 1, "Low": close - 1, "Close": close,
        "Volume": 100_000 + np.arange(len(dates)) * 10,
    }, index=dates.rename("Date"))
