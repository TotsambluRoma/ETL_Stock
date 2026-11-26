from datetime import timedelta

import numpy as np
import pandas as pd

from .config import FEATURES, PRICE_FIELDS
from .market import schedule


class QualityError(ValueError):
    """A batch cannot be published without correcting its input."""


REQUIRED = ("open", "high", "low", "close", "volume")
OPTIONAL = ("adj_close", "dividends", "stock_splits")


def gx_checks(frame: pd.DataFrame, expectations: list) -> list[str]:
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False, "metric_calculations": False}
    source = context.data_sources.add_pandas(name="stock_data")
    asset = source.add_dataframe_asset(name="batch")
    definition = asset.add_batch_definition_whole_dataframe("whole_batch")
    suite = context.suites.add(gx.ExpectationSuite(name="stock_quality"))
    for expectation in expectations:
        suite.add_expectation(expectation)
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name="before_load", data=definition, suite=suite)
    )
    result = validation.run(
        batch_parameters={"dataframe": frame},
        result_format={"result_format": "BASIC", "partial_unexpected_count": 5},
    )
    failed = [
        f"{item.expectation_config.type} ({item.expectation_config.kwargs.get('column', 'таблица')})"
        for item in result.results if not item.success
    ]
    if failed:
        raise QualityError("Great Expectations: " + ", ".join(failed))
    return [item.expectation_config.type for item in result.results]


def price_checks(frame: pd.DataFrame, start, end) -> list[str]:
    import great_expectations as gx

    checks = [gx.expectations.ExpectTableColumnsToMatchSet(column_set=["trade_date", *PRICE_FIELDS])]
    checks += [gx.expectations.ExpectColumnValuesToNotBeNull(column=field)
               for field in ("trade_date", *REQUIRED)]
    checks.append(gx.expectations.ExpectColumnValuesToBeUnique(column="trade_date"))
    checks += [gx.expectations.ExpectColumnValuesToBeBetween(column=field, min_value=0, strict_min=True)
               for field in ("open", "high", "low", "close")]
    checks.append(gx.expectations.ExpectColumnValuesToBeBetween(column="volume", min_value=0))
    passed = gx_checks(frame, checks)

    values = frame[list(REQUIRED)].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise QualityError("Цена или объём не являются конечным числом.")
    if frame["volume"].mod(1).ne(0).any() or frame["volume"].ge(2**63).any():
        raise QualityError("Объём должен быть целым числом в диапазоне BIGINT.")
    for optional in OPTIONAL:
        values = frame[optional].dropna().to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values < 0).any():
            raise QualityError(f"Неверное значение {optional}.")
    if ((frame["low"] > frame[["open", "close"]].min(axis=1))
        | (frame["high"] < frame[["open", "close"]].max(axis=1))).any():
        raise QualityError("Open и Close должны находиться между Low и High.")

    expected = set(schedule(start, end - timedelta(days=1)).index.date)
    actual = set(frame["trade_date"].dt.date)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise QualityError(f"Календарь NASDAQ: пропусков {len(missing)} {missing[:3]}, "
                           f"лишних дат {len(extra)} {extra[:3]}.")
    return passed + ["finite_values", "volume_integer", "ohlc", "session_dates"]


def feature_checks(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    import great_expectations as gx

    checks = [gx.expectations.ExpectColumnValuesToBeUnique(column="trade_date")]
    checks += [gx.expectations.ExpectColumnValuesToNotBeNull(column=feature) for feature in FEATURES]
    passed = gx_checks(frame, checks)
    if not np.isfinite(frame[list(FEATURES)].to_numpy()).all():
        raise QualityError("В признаках есть бесконечное или нечисловое значение.")
    return passed + ["finite_features"]
