FROM ubuntu:focal-20220801
RUN apt-get update -y\
    && apt-get install -y python3-pip \
    && apt-get install -y python3-dev build-essential
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
COPY requirements.txt /
EXPOSE 80/tcp
RUN pip3 install -r requirements.txt --no-cache-dir


COPY ./app app 
COPY ./.env ./.env 
WORKDIR /app/

RUN chmod +x /wait

RUN /wait \
    && python3 manage.py collectstatic --noinput \
    && python3 manage.py makemigrations \
    && python3 manage.py migrate

ENTRYPOINT ["gunicorn"]
CMD ["config.wsgi:application", "--bind", "backend:80", "--workers", "1", "--reload"]