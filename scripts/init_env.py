from base64 import urlsafe_b64encode
from pathlib import Path
from secrets import token_bytes, token_hex


root = Path(__file__).resolve().parents[1]
target = root / ".env"
values = []
for line in (root / ".env.example").read_text(encoding="utf-8").splitlines():
    if line and not line.startswith("#") and line.endswith("="):
        if line == "AIRFLOW_FERNET_KEY=":
            line += urlsafe_b64encode(token_bytes(32)).decode("ascii")
        else:
            line += token_hex(20)
    values.append(line)
try:
    with target.open("x", encoding="utf-8") as file:
        file.write("\n".join(values) + "\n")
except FileExistsError:
    print(".env уже существует.")
else:
    print(".env создан.")
