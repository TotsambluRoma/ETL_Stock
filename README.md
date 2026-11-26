# Конвейер биржевых котировок

Проект обрабатывает дневные котировки NASDAQ для AAPL, MSFT и NVDA. Исходные
снимки остаются в MinIO. Проверенные котировки, признаки и прогнозы поступают в
PostgreSQL. Небольшая сеть PyTorch учится с MSE и выдаёт поправку к текущей цене
закрытия.

## Запуск

Для сборки на x86-64 нужны Docker с Compose и Python для создания `.env`. На
Windows Docker Desktop использует WSL 2. После запуска Docker выполните из
каталога `stock-pipeline` (на Linux вместо `py` используйте `python3`):

```powershell
py scripts/init_env.py
docker compose up --build -d
```

Скрипт заполняет `.env` случайными паролями, сохраняя уже созданный файл. Первая
сборка загружает зависимости и компилирует зафиксированный выпуск MinIO. Команда
`docker compose ps` показывает состояние сервисов. Настройки расписания,
перекрытия и повторов находятся в `config/settings.yaml`, бумаги в
`config/tickers.csv`. Чтобы исключить бумагу из новых запусков, поставьте `false`
в столбце `active`.

| Сервис | Адрес по умолчанию | Вход |
|---|---|---|
| Airflow | `http://localhost:8080` | `AIRFLOW_UI_USER` и `AIRFLOW_UI_PASSWORD` из `.env` |
| MinIO | `https://localhost:9001` | `MINIO_ROOT_USER` и `MINIO_ROOT_PASSWORD` |
| Grafana | `http://localhost:3000` | `admin`, пароль `GRAFANA_PASSWORD` |
| Prometheus | `http://localhost:9090` | Интерфейс сбора метрик |
| Mailpit | `http://localhost:8025` | Входящие уведомления учебного стенда |

MinIO и PostgreSQL используют сертификаты локального центра из тома `tls`.
Сервисы конвейера проверяют этот центр автоматически. Для консоли MinIO тот же
сертификат можно получить командой
`docker compose cp minio:/tls/generated/ca.crt ./stock-pipeline-ca.crt`.
PostgreSQL принимает соединения из сети Compose; его порт не открыт на хосте.

## Два DAG

`stock_daily` запускается в 07:00 UTC. На пустой базе он начинает с даты из
справочника. Для ручной полной загрузки в Airflow нажмите **Trigger** у
`stock_daily` и задайте `mode: full`. Параметры `ticker`, `from_date` и
`through_date` позволяют ограничить запуск. Даты имеют вид `2024-12-31`, конечная
дата в интерфейсе включается. Конвейер берёт только завершившиеся сессии.

После первой загрузки вручную запустите `stock_train`. Он обучит по одной сети
на тикер и назначит её активной. Чтобы только сохранить версию, задайте
`activate: false`. Следующий `stock_daily` выпустит прогноз при наличии полной
строки признаков, активной модели и открытого окна до начала целевой сессии.
Если активной модели ещё нет, ETL всё равно запишет котировки и признаки.

В режиме `replay` задайте `snapshot_run_id` из `pipeline_runs`. При желании
укажите `ticker`. Конвейер обработает Parquet из MinIO без нового запроса к
Yahoo Finance. В этом режиме новый прогноз не публикуется. Для повторного запуска
после окончательного сбоя создайте новый запуск в Airflow.

Подготовленные данные можно посмотреть прямо в PostgreSQL:

```powershell
docker compose exec postgres psql -U postgres -d market -c "SELECT * FROM forecast_comparison ORDER BY forecast_date DESC LIMIT 10;"
```

`actual_close` появляется после загрузки факта за целевую дату. В `pipeline_runs`
видны номер запуска, итог и причины отклонений по бумагам. Airflow отправляет
копию результата в Mailpit. Панель Grafana «Конвейер котировок» показывает
длительность, последний статус, число отклонённых строк и отставание в сессиях.

## Проверка

Из контейнера можно запустить тесты и напрямую вызвать те же этапы, что
использует Airflow:

```powershell
docker compose exec airflow python -m pytest -q
docker compose exec airflow python -m pipeline etl --mode full --ticker AAPL --through-date 2024-12-31
```

Интеграционные тесты записывают синтетические котировки в хранилище. Запускайте их
на отдельном учебном экземпляре:

```powershell
docker compose exec -e PIPELINE_INTEGRATION=1 airflow python -m pytest --cov=pipeline
```

При окончании работы `docker compose down` останавливает сервисы, сохраняя тома.
