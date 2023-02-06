FROM python:3.10.7-slim-buster 

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install --no-install-recommends -y \
  # dependencies for building Python packages
  build-essential \
  # psycopg2 dependencies
  libpq-dev \
  python3-dev \
  pkg-config

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait

COPY ./etc/compose/admin_panel/requirements.txt /
RUN pip3 install -r requirements.txt --no-cache-dir

COPY ./src/admin_panel /app/src/admin_panel 
COPY ./etc/compose/admin_panel/start.sh /

WORKDIR /app/src/admin_panel/

RUN chmod +x /wait
RUN chmod +x /start.sh
