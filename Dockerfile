FROM apache/airflow:3.1.8-python3.12

ENV PYTHONPATH=/opt/stock \
    PYTHONUNBUFFERED=1 \
    GX_ANALYTICS_ENABLED=false

COPY --chown=airflow:root requirements*.txt /opt/stock/
# curl-cffi needs newer certifi and cffi than the Airflow constraints provide.
RUN curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt" \
    -o /tmp/airflow-constraints.txt \
 && sed -i '/^certifi==/d; /^cffi==/d' /tmp/airflow-constraints.txt \
 && pip install --no-cache-dir "apache-airflow==3.1.8" -r /opt/stock/requirements.txt \
    -r /opt/stock/requirements-test.txt \
    --constraint /tmp/airflow-constraints.txt \
 && pip install --no-cache-dir "torch==2.8.0+cpu" \
    --index-url https://download.pytorch.org/whl/cpu --extra-index-url https://pypi.org/simple \
 && pip check

COPY --chown=airflow:root pipeline /opt/stock/pipeline
COPY --chown=airflow:root dags /opt/stock/dags
COPY --chown=airflow:root tests /opt/stock/tests
COPY --chown=airflow:root scripts/start_airflow.py /opt/stock/scripts/start_airflow.py
WORKDIR /opt/stock
