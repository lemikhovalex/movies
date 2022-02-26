import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

# to meet the reqs of keeping code for data migration in 03_sqlit
# relative import is the only way

BATCH_SIZE = 256
PATH_TO_SQLITE = "../../03_sqlite_to_postgres/db.sqlite"


TABLE_SQLITE_PG = {
    "person": (
        [
            ("full_name", "full_name"),
            ("birth_date", "birth_date"),
            ("id", "id"),
            ("created_at", "created"),
        ]
    ),
    "person_film_work": (
        [
            ("film_work_id", "film_work_id"),
            ("person_id", "person_id"),
            ("role", "role"),
            ("created_at", "created"),
            ("id", "id"),
        ]
    ),
    "film_work": (
        [
            ("title", "title"),
            ("description", "description"),
            ("type", "type"),
            ("creation_date", "creation_date"),
            ("certificate", "certificate"),
            ("file_path", "file_path"),
            ("rating", "rating"),
            ("id", "id"),
            ("created_at", "created"),
        ]
    ),
    "genre": [
        ("name", "name"),
        ("description", "description"),
        ("id", "id"),
        ("created_at", "created"),
    ],
    "genre_film_work": [
        ("film_work_id", "film_work_id"),
        ("genre_id", "genre_id"),
        ("created_at", "created"),
        ("id", "id"),
    ],
}

load_dotenv("../../.env")
dsl = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "127.0.0.1",
    "port": 5432,
}


def relace_brackets(q: str) -> str:
    q = q.replace("'", "")
    q = q.replace("[", "")
    q = q.replace("]", "")
    return q


def check_table_names():
    with sqlite3.connect(PATH_TO_SQLITE) as sqlite_conn, psycopg2.connect(
        **dsl,
        cursor_factory=DictCursor,
    ) as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            pg_cursor.execute(
                "".join(
                    [
                        "SELECT table_name FROM information_schema.tables ",
                        "WHERE table_schema='content'",
                    ]
                )
            )

            sq_lite_curs = sqlite_conn.cursor()
            sq_lite_curs.execute(
                "SELECT name FROM sqlite_master WHERE type='table';",
            )

            sq_tables = sq_lite_curs.fetchmany(len(TABLE_SQLITE_PG) + 1)
            pg_tables = pg_cursor.fetchmany(len(TABLE_SQLITE_PG) + 1)
            assert len(pg_tables) == len(TABLE_SQLITE_PG)
            assert len(sq_tables) == len(TABLE_SQLITE_PG)
            for t_name in pg_tables:
                assert t_name not in sq_tables


def check_number_of_rows():
    pg_frmtbl = "SELECT COUNT(*) FROM content.{table};"
    sqlt_frmtbl = "SELECT COUNT(*) FROM {table};"
    with sqlite3.connect(PATH_TO_SQLITE) as sqlite_conn, psycopg2.connect(
        **dsl,
        cursor_factory=DictCursor,
    ) as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            sq_lite_curs = sqlite_conn.cursor()
            for table_name in TABLE_SQLITE_PG.keys():
                pg_cursor.execute(pg_frmtbl.format(table=table_name))
                sq_lite_curs.execute(sqlt_frmtbl.format(table=table_name))
                rows_pg = pg_cursor.fetchone()[0]
                rows_sqlite = sq_lite_curs.fetchone()[0]
                err_msg = "{table}, pg: {r_pg}, sqlite: {r_sq}".format(
                    r_pg=rows_pg, r_sq=rows_sqlite, table=table_name
                )
                assert rows_pg == rows_sqlite, err_msg
            sq_lite_curs.close()


def compare_2_coursors(pg_curs, sq_curs):
    terminate = False
    while not terminate:
        fetched_pg = pg_curs.fetchmany(BATCH_SIZE)
        fetched_sq = sq_curs.fetchmany(BATCH_SIZE)

        # check if this is the end of the query result
        if (len(fetched_pg) < BATCH_SIZE) or (len(fetched_sq) < BATCH_SIZE):
            terminate = True
        # parse data to output type
        # todo save parse
        for d_pg, d_sq in zip(fetched_pg, fetched_sq):
            msg = "pg != sq\n{_d1}!={_d2}".format(
                _d1=d_pg,
                _d2=d_sq,
            )
            assert d_pg[0] == d_sq[0], msg


def check_every_table_every_line():
    with sqlite3.connect(PATH_TO_SQLITE) as sqlite_conn, psycopg2.connect(
        **dsl,
        cursor_factory=DictCursor,
    ) as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            sq_lite_curs = sqlite_conn.cursor()
            for table_name, fields in TABLE_SQLITE_PG.items():
                pg_flds = [fld[1] for fld in fields]
                sq_flds = [fld[0] for fld in fields]
                q = "SELECT {fld} FROM content.{tbl} ORDER BY id DESC;".format(
                    fld=pg_flds,
                    tbl=table_name,
                )
                q = relace_brackets(q)

                pg_cursor.execute(q)
                q = "SELECT {flds} FROM {tbl} ORDER BY id DESC;".format(
                    flds=sq_flds,
                    tbl=table_name,
                )

                q = relace_brackets(q)

                sq_lite_curs.execute(q)
                compare_2_coursors(pg_cursor, sq_lite_curs)


if __name__ == "__main__":
    check_table_names()
    check_number_of_rows()
    check_every_table_every_line()
