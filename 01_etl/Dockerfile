FROM ubuntu:latest
RUN apt-get update -y\
    && apt-get install -y python3-pip \
    && apt-get install -y python3-dev build-essential
COPY requirements.txt /
EXPOSE 80/tcp
RUN pip3 install -r requirements.txt --no-cache-dir
COPY ./app app 
COPY ./.env ./.env 
WORKDIR /app/02_movies_admin
RUN python3 manage.py collectstatic --noinput
ENTRYPOINT ["gunicorn"]
CMD ["config.wsgi:application", "--bind", "backend:80", "--workers", "2", "--reload"]