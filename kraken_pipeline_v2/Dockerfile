FROM apache/airflow:2.9.3
USER airflow
RUN python -m pip uninstall -y kafka-python || true
RUN python -m pip install --no-cache-dir confluent-kafka==2.6.1 requests==2.32.3

