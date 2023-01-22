FROM apache/airflow:2.3.0-python3.10

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

USER root

RUN apt-get update && apt-get install --no-install-recommends -y \
    # dependencies for building Python packages
    build-essential \
    # psycopg2 dependencies
    libpq-dev \
    python3-dev \
    pkg-config

COPY requirements.txt /srv/app/
RUN chown -R airflow /srv/app/

USER airflow
RUN pip install -r /srv/app/requirements.txt --no-cache-dir

USER root
COPY . /srv/app/
RUN chown -R airflow /srv/app/
USER airflow

ARG DEBUG
RUN pip install -e /srv/app/
