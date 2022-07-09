FROM apache/airflow:2.3.2-python3.9
LABEL maintainer="Pedro Simas Neto"
USER root

## Atualiza SO
RUN apt-get update \
  && apt-get install -y --no-install-recommends \ 
        python3.9-dev \
        python3-pip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install pip install --no-cache-dir apache-airflow-providers-telegram==3.0.0 \
    apache-airflow[microsoft.mssql]==2.3.2
