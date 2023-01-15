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


def main(sqlite_conn: sqlite3.Connection, pg_conn, batch_size: int):
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