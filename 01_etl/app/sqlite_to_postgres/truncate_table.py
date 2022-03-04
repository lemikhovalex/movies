import psycopg2
from psycopg2.extras import DictCursor

from loaders import get_dsl

if __name__ == "__main__":

    with psycopg2.connect(
        **get_dsl("../../.env"),
        cursor_factory=DictCursor,
    ) as pg_conn:
        with pg_conn.cursor() as cursor:
            cursor.execute("TRUNCATE content.person_film_work CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.genre_film_work CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.genre CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.person CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.film_work CASCADE;")
            pg_conn.commit()
