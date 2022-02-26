import psycopg2
from psycopg2.extras import DictCursor

from loaders import get_dsl

SEP_LEN = 25
SEP = "_"


def display_table(conn, table: str):
    """Display table.

    Args:
        conn ([type]): connection to pg database
        table (str): name of the table
    """
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM content.{table_name};".format(
                table_name=table,
            ),
        )
        fetched = cursor.fetchmany(4)
        print(SEP_LEN * SEP)
        print(table)
        for record in fetched:
            print("\n")
            for field in record:
                print(field)


def main():
    """Iterate over Table and show content."""
    with psycopg2.connect(
        **get_dsl("../.env"),
        cursor_factory=DictCursor,
    ) as pg_conn:
        # tables = [
        #     "film_work",
        #     "person",
        #     "person_film_work",
        #     "genre",
        #     "genre_film_work",
        # ]
        # for table in tables:
        #     display_table(pg_conn, table)
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT role FROM content.person_film_work;")
            fetched = cursor.fetchall()
            fetched = [str(f) for f in fetched]
            print(set(fetched))


if __name__ == "__main__":
    main()
