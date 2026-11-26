import argparse
import json
import logging
import time
from uuid import uuid4

from . import workflow
from .config import read_settings
from .quality import QualityError


def step(name, function, item, settings):
    for attempt in range(settings.retry_count + 1):
        try:
            return workflow.run_step(name, function, item)
        except QualityError:
            raise
        except Exception:
            if attempt == settings.retry_count:
                raise
            time.sleep(settings.retry_delay_minutes * 60)


def main():
    parser = argparse.ArgumentParser(description="Запуск тех же этапов, что и в Airflow.")
    parser.add_argument("operation", choices=["etl", "train"])
    parser.add_argument("--mode", choices=["full", "incremental", "replay"], default="incremental")
    parser.add_argument("--ticker")
    parser.add_argument("--from-date")
    parser.add_argument("--through-date")
    parser.add_argument("--snapshot-run-id")
    parser.add_argument("--no-activate", action="store_false", dest="activate")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = read_settings()
    run_id = str(uuid4())
    for item in workflow.prepare_run(run_id, args.operation, vars(args)):
        try:
            if args.operation == "train":
                step("train", workflow.train_batch, item, settings)
            else:
                extracted = step("extract", workflow.extract_batch, item, settings)
                prepared = step("transform", workflow.transform_batch, extracted, settings)
                loaded = step("load", workflow.load_batch, prepared, settings)
                step("forecast", workflow.forecast_batch, loaded, settings)
        except Exception:
            # run_step has already recorded the error for this ticker.
            continue
    result = workflow.summarize_run(run_id)
    workflow.notify(result)
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))
    return 0 if result["status"] in workflow.GOOD else 1


if __name__ == "__main__":
    raise SystemExit(main())
