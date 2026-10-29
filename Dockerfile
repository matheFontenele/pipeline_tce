ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.10.4-python3.10
FROM ${AIRFLOW_BASE_IMAGE} AS runtime

ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.10

USER airflow
RUN mkdir -p /opt/airflow/pilot-data
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements-airflow.txt

COPY --chown=airflow:root src/pipeline_tce/ /opt/airflow/pipeline-src/pipeline_tce/
COPY --chown=airflow:root docker/airflow-init.sh /opt/airflow/scripts/airflow-init.sh
ENV PYTHONPATH="/opt/airflow/pipeline-src"

FROM runtime AS test
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.10
USER airflow
COPY requirements-test.txt /tmp/requirements-test.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements-test.txt
COPY --chown=airflow:root src/pipeline_tce/ /opt/airflow/pipeline-src/pipeline_tce/
ENV PYTHONPATH="/opt/airflow/pipeline-src"
CMD ["python", "-m", "pytest", "/opt/airflow/tests", "-q"]
