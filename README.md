# Online cinema api

This is a pet project based on Yandex-Practice course.
The main idea is to create back-end services for online cinema.
The project includes:
- Admin panel
- Endpoints for reading data
- Transfering data from initial SQLite database
- ETL from Postgres to Elastic Search

## To start up
Prefered way with docker compose
Start wth moving example `.env.sample` to `.env`.

Then run 

`docker-compose up -d --build`

To transfer data from SQLire to production database run inside container

`cd /app/sqlite_to_postgres && python3 load_data.py`

Finally you will end get admin panel with data in it.