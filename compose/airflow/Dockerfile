FROM apache/airflow:2.3.0-python3.10

USER root

RUN apt-get update && apt-get install --no-install-recommends -y \
    # dependencies for building Python packages
    build-essential \
    # psycopg2 dependencies
    libpq-dev \
    python3-dev \
    pkg-config

COPY requirements.txt /srv/src/
RUN chown -R airflow /srv/src/

USER airflow
RUN pip install -r /srv/src/requirements.txt --no-cache-dir

COPY . /srv/src/
ARG DEBUG
RUN if [ "${DEBUG}" = "1" ] ; then pip install /srv/src/ ; else pip install -e /srv/src/ ; fi
