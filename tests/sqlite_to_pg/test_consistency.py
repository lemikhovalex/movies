import sqlite3
from typing import List, Type

from etl.sqlite_to_postgres.loaders import PostgresSaver, SQLiteDownLoader
from etl.sqlite_to_postgres.tables import (
    Filmwork,
    Genre,
    GenreFilmwork,
    Person,
    PersonFilmwork,
    SelectableFromSQLite,
)

BATCH_SIZE = 256
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



def relace_brackets(q: str) -> str:
    q = q.replace("'", "")
    q = q.replace("[", "")
    q = q.replace("]", "")
    return q


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



def test_table_names(pg_conn, sqlite_conn: sqlite3.Connection):

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


def test_etl_process(pg_conn, sqlite_conn: sqlite3.Connection):
    down_loader = SQLiteDownLoader(connect=sqlite_conn)
    up_loader = PostgresSaver(connect=pg_conn)
    # load and save tables in cycle
    tables: List[Type[SelectableFromSQLite]] = [
        Genre,
        Person,
        Filmwork,
        GenreFilmwork,
        PersonFilmwork,
    ]
    for table in tables:
        table_loader = down_loader.load_class(
            constructor=table,
            batch_size=BATCH_SIZE,
        )
        for batch in table_loader:
            up_loader.insert_to_table(
                batch,
                table_name=table.table_name,
            )
    pg_conn.commit()


def test_number_of_rows(pg_conn, sqlite_conn: sqlite3.Connection):
    pg_frmtbl = "SELECT COUNT(*) FROM content.{table};"
    sqlt_frmtbl = "SELECT COUNT(*) FROM {table};"
    
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


def testr_every_table_every_line(pg_conn, sqlite_conn: sqlite3.Connection):

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
