from datetime import UTC, datetime
from threading import Event

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import GaugeMetricFamily

from . import db
from .market import last_closed, schedule


class PipelineMetrics:
    def collect(self):
        runs = GaugeMetricFamily("stock_pipeline_runs", "Finished runs", labels=["operation", "status"])
        status = GaugeMetricFamily("stock_pipeline_last_status", "Latest result", labels=["operation", "status"])
        duration = GaugeMetricFamily("stock_pipeline_last_duration_seconds", "Duration of last run",
                                     labels=["operation"])
        rejected = GaugeMetricFamily("stock_pipeline_last_rejected_rows", "Rejected rows in last run",
                                     labels=["operation"])
        freshness = GaugeMetricFamily("stock_pipeline_quote_lag_sessions", "Missing recent NASDAQ sessions",
                                      labels=["ticker"])
        with db.connect() as conn:
            state_counts = conn.execute("""
                SELECT operation, status, count(*) AS total FROM pipeline_runs
                WHERE finished_at IS NOT NULL GROUP BY operation, status
            """).fetchall()
            last_runs = conn.execute("""
                SELECT DISTINCT ON (operation) operation, status, details,
                       extract(epoch FROM finished_at - started_at) AS seconds
                FROM pipeline_runs WHERE finished_at IS NOT NULL
                ORDER BY operation, finished_at DESC
            """).fetchall()
            quote_dates = conn.execute("""
                SELECT i.ticker, max(p.trade_date) AS latest FROM instruments AS i
                JOIN daily_prices AS p USING (instrument_id)
                WHERE i.active GROUP BY i.ticker
            """).fetchall()

        for row in state_counts:
            runs.add_metric([row["operation"], row["status"]], row["total"])
        for row in last_runs:
            operation = [row["operation"]]
            status.add_metric([*operation, row["status"]], 1)
            duration.add_metric(operation, float(row["seconds"]))
            rejected.add_metric(operation, sum(
                result.get("rejected_rows", 0) for result in row["details"]["tickers"].values()
            ))
        expected = last_closed(datetime.now(UTC))
        for row in quote_dates:
            since = row["latest"]
            lag = max(0, len(schedule(since, expected)) - 1) if since <= expected else 0
            freshness.add_metric([row["ticker"]], lag)
        yield runs
        yield status
        yield duration
        yield rejected
        yield freshness


if __name__ == "__main__":
    registry = CollectorRegistry()
    registry.register(PipelineMetrics())
    start_http_server(9101, addr="0.0.0.0", registry=registry)
    Event().wait()
