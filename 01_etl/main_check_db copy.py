import psycopg2
from psycopg2.extras import DictCursor

from app.sqlite_to_postgres.loaders.settings import get_dsl

if __name__ == "__main__":
    with psycopg2.connect(
        **get_dsl(".env"),
        cursor_factory=DictCursor,
    ) as pg_conn:
        with pg_conn.cursor() as cursor:
            with open("fw_persons.sql") as f:
                query = f.read()
            cursor.execute(
                query,
            )
            print(cursor.fetchall())
