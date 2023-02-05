# Online cinema api

This is a pet project based on Yandex-Practice course.
The main idea is to create back-end services for online cinema.
The project includes:
- Admin panel
- Transfering data from initial SQLite database
- ETL from Postgres to Elastic Search
- Endpoints for reading data fast, from Elastic search

## To start up
Prefered way with docker compose
Start wth moving example `sample.env` to `.env`.

`docker-compose up -d --build`

To transfer data from SQLire to production database run inside container

The admin panel can be found at `http://127.0.0.1/admin/`. Default creadentials can be found in .env. Feel free to check `admin`, `123qwe` first

The air flow webserver at `http://127.0.0.1/airflow/`. Default credentials are `airflow`, `airflow`

To run ETL from sqlite to Postgres go to airflow worker container and
- `cd /srv/app/`
- `python`
- `from etl.sqlite_to_postgres import main`
- `main()`

After that you can observe data in admin panel

To run ETL from Postgres to Elsatic search trigger graph `http://127.0.0.1/airflow/dags/movies_etl_pg_to_es`