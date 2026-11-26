import json
import os
from pathlib import Path


file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
file.parent.mkdir(parents=True, exist_ok=True)
user = os.environ["AIRFLOW_UI_USER"]
temporary = file.with_suffix(".new")
temporary.write_text(json.dumps({user: os.environ["AIRFLOW_UI_PASSWORD"]}), encoding="utf-8")
temporary.chmod(0o600)
temporary.replace(file)
os.execvp("airflow", ["airflow", "standalone"])
