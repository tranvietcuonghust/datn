FROM apache/airflow:2.7.3-python3.10
LABEL maintainer="Data Engineering Team"
LABEL email="taen@tokenmetrics.com"
LABEL email="jake@tokenmetrics.com"

WORKDIR ${AIRFLOW_HOME}

COPY vnaddress /home/airflow/.local/lib/python3.10/site-packages/vnaddress
COPY vnaddress-1.0.5.dist-info /home/airflow/.local/lib/python3.10/site-packages/vnaddress-1.0.5.dist-info
COPY requirements.txt .
# COPY requirements_coingecko.txt .

RUN pip install --no-cache-dir "apache-airflow==2.7.3"
RUN pip install -r requirements.txt
RUN pip install -U sentence-transformers
RUN pip install sentencepiece   
RUN pip install protobuf==3.20.0
# /.local/lib/python3.10/site-packages 
COPY . .
USER root
ENV PIP_USER=false
# RUN apt-get update && apt-get install -y \
#     gcc python3-dev \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*
# RUN mkdir -p ${AIRFLOW_HOME}/env/coingeko_env
# RUN python3 -m venv ${AIRFLOW_HOME}/env/coingeko_env
# RUN ${AIRFLOW_HOME}/env/coingeko_env/bin/pip install ${AIRFLOW_HOME}/dags/coingecko/utils
# RUN ${AIRFLOW_HOME}/env/coingeko_env/bin/pip install -r requirements_coingecko.txt
ENV PIP_USER=true
USER airflow

