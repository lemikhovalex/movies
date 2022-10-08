FROM python:3.10-alpine

RUN addgroup -S app && adduser -S app app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk add -U --no-cache build-base && \
    apk add --no-cache bash && \
    apk del build-base

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

VOLUME /app

CMD /start.sh