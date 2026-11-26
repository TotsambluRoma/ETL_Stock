from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task, task_group

from pipeline.config import read_settings


settings = read_settings()
retry_delay = timedelta(minutes=settings.retry_delay_minutes)
retry_options = {"retries": settings.retry_count, "retry_delay": retry_delay}


def run_id():
    dag_run = get_current_context()["dag_run"]
    return str(uuid5(NAMESPACE_URL, f"stock-pipeline/{dag_run.dag_id}/{dag_run.run_id}"))


@task(**retry_options)
def prepare(operation: str):
    from pipeline.workflow import prepare_run

    try:
        return prepare_run(run_id(), operation, get_current_context()["params"])
    except ValueError as error:
        raise AirflowFailException(str(error)) from error


def execute(name, function, item):
    from pipeline.quality import QualityError
    from pipeline.workflow import run_step

    try:
        return run_step(name, function, item)
    except QualityError as error:
        raise AirflowFailException(str(error)) from error


@task(trigger_rule="all_done", **retry_options)
def finish():
    from pipeline.workflow import GOOD, notify, summarize_run

    result = summarize_run(run_id())
    notify(result)
    if result["status"] not in GOOD:
        raise AirflowFailException(f"Результат запуска: {result['status']}. ID {result['run_id']}")


@task_group
def process_ticker(item):
    @task(**retry_options)
    def extract(item):
        from pipeline.workflow import extract_batch

        return execute("extract", extract_batch, item)

    @task(**retry_options)
    def transform(item):
        from pipeline.workflow import transform_batch

        return execute("transform", transform_batch, item)

    @task(**retry_options)
    def load(item):
        from pipeline.workflow import load_batch

        return execute("load", load_batch, item)

    @task(**retry_options)
    def forecast(item):
        from pipeline.workflow import forecast_batch

        return execute("forecast", forecast_batch, item)

    forecast(load(transform(extract(item))))


@task(**retry_options)
def train(item):
    from pipeline.workflow import train_batch

    return execute("train", train_batch, item)


@dag(
    dag_id="stock_daily", start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule=settings.schedule, catchup=False, max_active_runs=1,
    params={
        "mode": Param("incremental", enum=["incremental", "full", "replay"]),
        "ticker": Param(None, type=["null", "string"]),
        "from_date": Param(None, type=["null", "string"], format="date"),
        "through_date": Param(None, type=["null", "string"], format="date"),
        "snapshot_run_id": Param(None, type=["null", "string"], format="uuid"),
    },
    tags=["stocks", "etl"],
)
def stock_daily():
    process_ticker.expand(item=prepare("etl")) >> finish()


@dag(
    dag_id="stock_train", start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule=None, catchup=False, max_active_runs=1,
    params={
        "ticker": Param(None, type=["null", "string"]),
        "through_date": Param(None, type=["null", "string"], format="date"),
        "activate": Param(True, type="boolean"),
    },
    tags=["stocks", "ml"],
)
def stock_train():
    train.expand(item=prepare("train")) >> finish()


stock_daily()
stock_train()
