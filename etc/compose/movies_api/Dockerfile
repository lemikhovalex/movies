FROM python:3.10.7-slim-buster 

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential

COPY ./etc/compose/movies_api/requirements.txt /
RUN pip install --upgrade pip
RUN pip3 install -r /requirements.txt --no-cache-dir

COPY ./src/movies_api/ /app/

WORKDIR /app/

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "main:app"]
