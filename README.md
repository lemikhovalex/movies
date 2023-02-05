# Online cinema api

This is a pet project based on Yandex-Practice course.
The main idea is to create back-end services for online cinema.
The project includes:
- Admin panel
- Transfering data from initial SQLite database
- ETL from Postgres to Elastic Search
- Endpoints for reading data fast, from Elastic search

## To start up

### Docker
Prefered way with docker compose
Start wth moving example `sample.env` to `.env`.

`docker-compose up -d --build`
### Add some data
To to add some data go to airflow worker container and
- `cd /srv/app/`
- `python`
- `from etl.sqlite_to_postgres import main`
- `main()`

After that you can observe data in admin panel

## Check some pages in browser

The admin panel can be found at `http://127.0.0.1/admin/`. Default creadentials can be found in .env. Feel free to check `admin`, `123qwe` first

The air flow webserver at `http://127.0.0.1/airflow/`. Default credentials are `airflow`, `airflow`


To run ETL from Postgres to Elsatic search trigger graph `http://127.0.0.1/airflow/dags/movies_etl_pg_to_es`


## To develop
### Requirements
For local development and typing support it's reccomended to install all packages for all images as follows:

`pip install -r requirements-dev.txt -r etc/compose/admin_panel/requirements.txt  -r etc/compose/airflow/requirements.txt`

### Docker
There is dedicated `sample.docker-compose.override.yml` file to separate production from development.
For the ease of development there is an opption to mount directories with code and edit code inside container.
To enable it jsut
- `cp sample.docker-compose.override.yml docker-compose.override.yml`
- `docekr-compose up -d`

And you changes will appear in containers

### Run tests

Go to `airflow-worker` container and
- `cd /srv/app/`
- `sh test.sh`
