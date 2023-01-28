import sqlite3
from typing import List, Optional, Type

import psycopg2
from psycopg2.extras import DictCursor

from etl.config import CONFIG
from etl.sqlite_to_postgres.loaders import PostgresSaver, SQLiteDownLoader
from etl.sqlite_to_postgres.tables import (
    Filmwork,
    Genre,
    GenreFilmwork,
    Person,
    PersonFilmwork,
    SelectableFromSQLite,
)


def main(
    sqlite_conn: Optional[sqlite3.Connection] = None,
    pg_conn=None,
    batch_size: int = 128,
):
    close_pg_conn = False
    close_sqlite_conn = False
    if pg_conn is None:
        dsl = {
            "dbname": CONFIG.db_name,
            "user": CONFIG.db_user,
            "password": CONFIG.db_password,
            "host": CONFIG.db_host,
            "port": CONFIG.db_port,
        }
        pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        close_pg_conn = True

    if sqlite_conn is None:
        sqlite_conn = sqlite3.connect("tests/db.sqlite")
        close_sqlite_conn = True

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
            batch_size=batch_size,
        )
        for batch in table_loader:
            up_loader.insert_to_table(
                batch,
                table_name=table.table_name,
            )
    pg_conn.commit()

    if close_pg_conn:
        pg_conn.close()
    if close_sqlite_conn:
        sqlite_conn.close()
