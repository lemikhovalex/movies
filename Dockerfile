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

COPY requirements.txt /
EXPOSE 80/tcp
RUN pip3 install -r requirements.txt --no-cache-dir

COPY ./ app 
COPY ./.env .app/.env 
COPY ./start.sh /

WORKDIR /app/

RUN chmod +x /wait
RUN chmod +x /start.sh
RUN chmod +x test.sh

VOLUME /app
RUN pip install -e .
CMD /start.sh
